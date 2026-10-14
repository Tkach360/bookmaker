package rmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

// тип RPCResult определяет результат получения ответа от сервера
// он содержит ошибку, которая произошла на стороне клиента
type RPCResult struct {
	Response RPCResponse
	Err      error
}

type RPCClient struct {
	conn *amqp.Connection

	ResponsesQueue amqp.Queue // очередь RabbitMQ, в которой будут ответы на запросы этого микросервиса
	consumer       *amqp.Channel

	producer *amqp.Channel

	methodsServices map[RPCMethodName]string // методы и очереди для них
	methodsMu       *sync.RWMutex

	// мапа для каналов ожидания ответа
	// correlationID - ключ, канал с ответом - значение
	pending   map[string]chan RPCResult
	stopErr   error       // не nil после остановки, защищено pendingMu
	closing   atomic.Bool // выставляется в Close
	closeOnce sync.Once
	pendingMu *sync.Mutex
}

func NewClient(conn *amqp.Connection, respQueueName string) (*RPCClient, error) {
	consumer, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("creating channel: %w", err)
	}

	producer, err := conn.Channel()
	if err != nil {
		consumer.Close()
		return nil, fmt.Errorf("creating channel: %w", err)
	}

	q, err := consumer.QueueDeclare(respQueueName, false, true, true, false, nil)
	if err != nil {
		consumer.Close()
		producer.Close()
		return nil, fmt.Errorf("declare queue: %w", err)
	}

	rpc := &RPCClient{
		conn:            conn,
		consumer:        consumer,
		producer:        producer,
		ResponsesQueue:  q,
		methodsServices: make(map[RPCMethodName]string),
		methodsMu:       &sync.RWMutex{},
		pending:         make(map[string]chan RPCResult),
		pendingMu:       &sync.Mutex{},
	}

	return rpc, nil
}

// добавить новый удаленный метод
func (r *RPCClient) AddMethod(mname RPCMethodName, qname string) {
	r.methodsMu.Lock()
	defer r.methodsMu.Unlock()
	r.methodsServices[mname] = qname
}

// запуск прослушивания ответов
// errCh - канал в который будут записываться ошибки при работе воркера
func (r *RPCClient) Start(ctx context.Context, errCh chan<- error) error {
	deliveries, err := r.consumer.Consume(
		r.ResponsesQueue.Name,
		"", false, false, false, false, nil,
	)
	if err != nil {
		return fmt.Errorf("starting consume: %w", err)
	}

	go r.responseListener(ctx, deliveries, errCh)

	return nil
}

// отправить сообщение без ожидания ответа (отправил и забыл)
// - r - клиент, через которого отправляем
// - mname - имя вызываемого метода
// - payload - аргументы
func Send[T any](ctx context.Context, r *RPCClient, mname RPCMethodName, payload T) error {
	request, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshaling payload json: %w", err)
	}

	r.methodsMu.RLock()
	queue, ok := r.methodsServices[mname]
	r.methodsMu.RUnlock()
	if !ok {
		return fmt.Errorf("unknown rpc send method %q", mname)
	}

	err = r.producer.PublishWithContext(
		ctx,
		"",
		queue,
		false,
		false,
		amqp.Publishing{
			Type:          string(mname),
			ContentType:   "application/json",
			CorrelationId: uuid.New().String(),
			ReplyTo:       "", // пусто, потому что ответ не предполагается
			Body:          request,
		},
	)
	if err != nil {
		return fmt.Errorf("publishing message: %w", err)
	}

	return nil
}

// вызов удаленной функции
// отправляет сообщение и возвращает канал, в котором появится ответ
// - mname - имя вызываемого метода
// - payload - аргументы
// возвращает:
// - канал, в которо появится ответ
// - correlationId
// - ошибку
func (r *RPCClient) call(ctx context.Context, mname RPCMethodName, payload json.RawMessage) (<-chan RPCResult, string, error) {
	corID := uuid.New().String()

	r.methodsMu.RLock()
	queue, ok := r.methodsServices[mname]
	if !ok {
		r.methodsMu.RUnlock()
		return nil, "", fmt.Errorf("unknown rpc call method %q", mname)
	}
	r.methodsMu.RUnlock()

	resultCh := make(chan RPCResult, 1)
	r.pendingMu.Lock()
	if r.stopErr != nil {
		err := r.stopErr
		r.pendingMu.Unlock()
		return nil, "", err
	}
	r.pending[corID] = resultCh
	r.pendingMu.Unlock()

	err := r.producer.PublishWithContext(
		ctx,
		"",
		queue,
		false,
		false,
		amqp.Publishing{
			Type:          string(mname),
			ContentType:   "application/json",
			CorrelationId: corID,
			ReplyTo:       r.ResponsesQueue.Name,
			Body:          payload,
		},
	)

	if err != nil {
		r.removePending(corID)
		return nil, "", fmt.Errorf("publishing message: %w", err)
	}

	return resultCh, corID, nil
}

func (r *RPCClient) removePending(id string) {
	r.pendingMu.Lock()
	delete(r.pending, id)
	r.pendingMu.Unlock()
}

// вызов удаленной функции
// отправляет сообщение и ждет ответа
// - r - клиент, у которого вызываем (обход того, что в Go нельзя делать методы с дженериками)
// - mname - имя вызываемого метода
// - payload - аргументы
// всегда возвращает непустой статус
func Call[T, B any](ctx context.Context, r *RPCClient, mname RPCMethodName, payload T) (*B, RPCResponseStatus, error) {
	request, err := json.Marshal(payload)
	if err != nil {
		return nil, STATUS_ERROR, fmt.Errorf("marshaling payload json: %w", err)
	}

	resultCh, corID, err := r.call(ctx, mname, request)
	if err != nil {
		return nil, STATUS_ERROR, fmt.Errorf("publish msg: %w", err)
	}
	defer r.removePending(corID)

	select {
	case result := <-resultCh:
		if result.Err != nil {
			return nil, STATUS_ERROR, fmt.Errorf("error receiving response: %w", result.Err)
		}

		response := result.Response
		if response.Err != "" {
			return nil, response.Status, fmt.Errorf("service error: %s", response.Err)
		}

		var data B
		if len(response.Payload) == 0 {
			return &data, response.Status, nil
		}

		if err := json.Unmarshal(response.Payload, &data); err != nil {
			return nil, response.Status, fmt.Errorf("unmarshaling: %w", err)
		}

		return &data, response.Status, nil

	case <-ctx.Done():
		return nil, STATUS_ERROR, ctx.Err()
	}
}

// воркер, который читает ответы от других микросервисов и помещает их в соответствующие каналы
// ошибки записывает в errCh
func (r *RPCClient) responseListener(ctx context.Context, deliveries <-chan amqp.Delivery, errCh chan<- error) {
	for {
		select {
		case <-ctx.Done():
			r.Close()
			reportErr(errCh, ctx.Err())
			return
		case d, ok := <-deliveries:
			if !ok {
				if r.closing.Load() {
					r.failAllPending(ErrClientClosed)
					return
				}
				err := fmt.Errorf("consumer channel closed for queue %s", r.ResponsesQueue.Name)
				r.failAllPending(err)
				reportErr(errCh, err)
				r.Close()
				return
			}

			r.pendingMu.Lock()
			ch, exists := r.pending[d.CorrelationId]
			delete(r.pending, d.CorrelationId)
			r.pendingMu.Unlock()

			if exists {
				var res RPCResult
				res.Err = json.Unmarshal(d.Body, &res.Response)
				select {
				case ch <- res:
				default: // буфер уже занят, дубликат
				}
			}
			d.Ack(false)
		}
	}
}

var ErrClientClosed = errors.New("rpc client closed")

func (r *RPCClient) failAllPending(err error) {
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()
	if r.stopErr == nil {
		r.stopErr = err
	}
	for id, ch := range r.pending {
		select {
		case ch <- RPCResult{Err: err}:
		default:
		}
		delete(r.pending, id)
	}
}

func (r *RPCClient) Close() error {
	var err error
	r.closeOnce.Do(func() {
		r.closing.Store(true)
		err = errors.Join(r.consumer.Close(), r.producer.Close())
		r.failAllPending(ErrClientClosed)
	})
	if err != nil {
		return fmt.Errorf("closing RPCClient: %w", err)
	}
	return nil
}

func reportErr(errCh chan<- error, err error) {
	select {
	case errCh <- err:
	default:
	}
}
