package rmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

// имя rpc метода, который будет вызываться на сервере
type RPCMethodName string

type RPCResponseStatus string

// тип RPCResponse определяет ответ от сервера
// Err - ошибка, произошедшая на стороне сервера
type RPCResponse struct {
	Err     string            `json:"error"`
	Status  RPCResponseStatus `json:"status"`
	Payload json.RawMessage   `json:"payload,omitempty"`
}

// обработчик "сырого" запроса: принимает тело запроса и возвращает
// тело ответа (уже сериализованный RPCResponse).
// nil в ответе означает «результата нет» (клиенту уйдёт пустой RPCResponse)
// если вернулась ошибка, клиенту уйдёт RPCResponse с заполненным Err
type Handler func(raw []byte) ([]byte, error)

const (
	defaultPrefetch = 16
	publishTimeout  = 5 * time.Second
)

var ErrServerClosed = errors.New("rpc server closed")

type RPCServer struct {
	// очередь RabbitMQ, из которой читаем запросы
	RequestsQueue string
	// максимум одновременно обрабатываемых запросов.
	// менять нужно до вызова Start.
	Prefetch int

	consumer    *amqp.Channel
	consumerTag string
	producer    *amqp.Channel

	handlers map[RPCMethodName]Handler
	mu       sync.RWMutex // защищает handlers

	stateMu   sync.Mutex // защищает started и согласует Start с Close
	started   bool
	closing   atomic.Bool
	closeOnce sync.Once
	done      chan struct{}  // закрывается в Close
	wg        sync.WaitGroup // listener + выполняющиеся обработчики
}

// создает сервер и объявляет очередь запросов.
// соединение сервер не закрывает: им владеет тот, кто его создал.
func NewServer(conn *amqp.Connection, requestsQueue string) (*RPCServer, error) {
	consumer, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("creating consumer channel: %w", err)
	}

	producer, err := conn.Channel()
	if err != nil {
		consumer.Close()
		return nil, fmt.Errorf("creating producer channel: %w", err)
	}

	// durable, не exclusive и не auto-delete: очередь переживает перезапуск сервиса
	_, err = consumer.QueueDeclare(requestsQueue, true, false, false, false, nil)
	if err != nil {
		consumer.Close()
		producer.Close()
		return nil, fmt.Errorf("declare queue: %w", err)
	}

	return &RPCServer{
		RequestsQueue: requestsQueue,
		Prefetch:      defaultPrefetch,
		consumer:      consumer,
		consumerTag:   "rpc-server-" + uuid.NewString(),
		producer:      producer,
		handlers:      make(map[RPCMethodName]Handler),
		done:          make(chan struct{}),
	}, nil
}

// запускает прослушивание запросов в отдельной горутине
// каждый запрос обрабатывается в своей горутине, их число ограничено Prefetch.
// остановка: отмена ctx или вызов Close.
// errCh - канал, в который будут записываться ошибки при работе воркера
func (r *RPCServer) Start(ctx context.Context, errCh chan<- error) error {
	r.stateMu.Lock()
	defer r.stateMu.Unlock()

	if r.closing.Load() {
		return ErrServerClosed
	}
	if r.started {
		return errors.New("rpc server already started")
	}

	if err := r.consumer.Qos(r.Prefetch, 0, false); err != nil {
		return fmt.Errorf("setting qos: %w", err)
	}

	deliveries, err := r.consumer.Consume(
		r.RequestsQueue,
		r.consumerTag,
		false, false, false, false, nil,
	)
	if err != nil {
		return fmt.Errorf("starting consume: %w", err)
	}

	r.started = true

	r.wg.Add(1)
	go r.requestsListener(deliveries, errCh)

	go func() {
		select {
		case <-ctx.Done():
			if err := r.Close(); err != nil {
				reportErr(errCh, err)
			}
		case <-r.done:
		}
	}()

	return nil
}

// воркер, который принимает запросы и запускает обработчики
func (r *RPCServer) requestsListener(deliveries <-chan amqp.Delivery, errCh chan<- error) {
	defer r.wg.Done()

	// цикл завершается, когда consumer отменён (Close) или канал закрыт
	for d := range deliveries {
		r.wg.Add(1) // безопасно: listener сам держит счётчик
		go func(d amqp.Delivery) {
			defer r.wg.Done()
			r.handle(d, errCh)
		}(d)
	}

	// если это не штатный Close, значит канал закрылся сам (ошибка, обрыв)
	if !r.closing.Load() {
		reportErr(errCh, fmt.Errorf("consumer channel closed for queue %s", r.RequestsQueue))
		go r.Close() // в отдельной горутине: Close ждёт завершения listener'а
	}
}

// обрабатывает один запрос: вызывает обработчик, отвечает (если нужно), подтверждает
func (r *RPCServer) handle(d amqp.Delivery, errCh chan<- error) {
	// подтверждаем в любом случае, чтобы не получить бесконечную повторную
	// доставку "ядовитого" сообщения
	defer func() {
		if err := d.Ack(false); err != nil {
			reportErr(errCh, fmt.Errorf("ack: %w", err))
		}
	}()

	r.mu.RLock()
	handler, ok := r.handlers[RPCMethodName(d.Type)]
	r.mu.RUnlock()

	var (
		res []byte
		err error
	)
	if !ok {
		err = fmt.Errorf("unknown rpc method %q", d.Type)
	} else {
		res, err = safeCall(handler, d.Body)
	}

	if err != nil {
		reportErr(errCh, fmt.Errorf("method %q: %w", d.Type, err))
	}

	// если ReplyTo пусто, значит ответ не требуется
	if d.ReplyTo == "" {
		return
	}

	switch {
	case err != nil:
		res = errorResponse(err)
	case res == nil:
		res = emptyResponse()
	}

	r.reply(d, res, errCh)
}

// публикует ответ в очередь, указанную клиентом в ReplyTo
func (r *RPCServer) reply(d amqp.Delivery, body []byte, errCh chan<- error) {
	// свой контекст: ответ на уже принятый запрос нужно отправить,
	// даже если ctx сервера уже отменён
	ctx, cancel := context.WithTimeout(context.Background(), publishTimeout)
	defer cancel()

	err := r.producer.PublishWithContext(
		ctx,
		"",
		d.ReplyTo,
		false,
		false,
		amqp.Publishing{
			ContentType:   "application/json",
			CorrelationId: d.CorrelationId,
			Body:          body,
		},
	)
	if err != nil {
		reportErr(errCh, fmt.Errorf("publish response: %w", err))
	}
}

// Close перестаёт принимать новые запросы, дожидается уже выполняющихся
// обработчиков (они успевают ответить и подтвердить сообщения) и закрывает
// каналы соединение не закрывается, повторные вызовы безопасны
// нельзя вызывать из самих обработчиков: Close ждёт их завершения
func (r *RPCServer) Close() error {
	var err error

	r.closeOnce.Do(func() {
		r.stateMu.Lock()
		r.closing.Store(true)
		started := r.started
		r.stateMu.Unlock()

		close(r.done)

		var cancelErr error
		if started {
			// новые доставки прекращаются, уже полученные дочитываются
			cancelErr = r.consumer.Cancel(r.consumerTag, false)
			if cancelErr != nil {
				_ = r.consumer.Close() // принудительно закрываем deliveries
			}
		}

		r.wg.Wait() // listener и все выполняющиеся обработчики

		err = errors.Join(cancelErr, r.consumer.Close(), r.producer.Close())
	})

	if err != nil {
		return fmt.Errorf("closing RPCServer: %w", err)
	}
	return nil
}

// установить новый обработчик
func (r *RPCServer) HandleRaw(name string, handler Handler) {
	r.mu.Lock()
	r.handlers[RPCMethodName(name)] = handler
	r.mu.Unlock()
}

// установить новый обработчик, но с оберткой
func RegisterHandler[T, B any](r *RPCServer, mname RPCMethodName, handler func(T) (B, RPCResponseStatus, error)) {
	r.HandleRaw(string(mname), HandlerWrap(handler))
}

// установить новый обработчик без возвращаемого значения
func RegisterHandlerNoRes[T any](r *RPCServer, mname RPCMethodName, handler func(T) error) {
	r.HandleRaw(string(mname), HandlerWrapNoRes(handler))
}

// обертка над обработчиком, десериализует аргументы из JSON в T,
// вызывает собственно обработчик, помещает ответ в RPCResponse и
// сериализует его в json.
// ошибка обработчика не теряется: она попадает в RPCResponse.Err
func HandlerWrap[T, B any](f func(T) (B, RPCResponseStatus, error)) Handler {
	return func(raw []byte) ([]byte, error) {
		var arg T
		if err := json.Unmarshal(raw, &arg); err != nil {
			return nil, fmt.Errorf("unmarshaling json arg: %w", err)
		}

		res, status, err := f(arg)
		if err != nil {
			return json.Marshal(RPCResponse{Err: err.Error(), Status: status})
		}

		rawRes, err := json.Marshal(res)
		if err != nil {
			return nil, fmt.Errorf("marshaling json res: %w", err)
		}

		rawResponse, err := json.Marshal(RPCResponse{Status: status, Payload: rawRes})
		if err != nil {
			return nil, fmt.Errorf("marshaling json response: %w", err)
		}

		return rawResponse, nil
	}
}

// обертка над обработчиком, который не предполагает возврата значения,
// десериализует аргументы из JSON в T, вызывает собственно обработчик.
// Результат всегда nil; если клиент всё же ждёт ответ, ему уйдёт пустой
// RPCResponse (или RPCResponse с ошибкой, если обработчик вернул error)
func HandlerWrapNoRes[T any](f func(T) error) Handler {
	return func(raw []byte) ([]byte, error) {
		var arg T
		if err := json.Unmarshal(raw, &arg); err != nil {
			return nil, fmt.Errorf("unmarshaling json arg: %w", err)
		}

		return nil, f(arg)
	}
}

// вызывает обработчик, превращая панику в ошибку
func safeCall(h Handler, raw []byte) (res []byte, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("handler panic: %v", p)
		}
	}()
	return h(raw)
}

// сериализованный RPCResponse с ошибкой
func errorResponse(err error) []byte {
	b, mErr := json.Marshal(RPCResponse{Err: err.Error()})
	if mErr != nil {
		return []byte(`{"error":"internal error"}`)
	}
	return b
}

// сериализованный пустой RPCResponse
func emptyResponse() []byte {
	b, _ := json.Marshal(RPCResponse{})
	return b
}
