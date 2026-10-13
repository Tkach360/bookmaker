package rmq

import (
	"bookmaker/shared/dto"
	"context"
	"encoding/json"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RPCServer struct {
	conn *amqp.Connection

	ResponsesQueue string // очередь RabbitMQ, в которую будем помещать ответы
	consumer       *amqp.Channel

	RequestsQueue string // очередь RabbitMQ, из которой будем читать запросы
	producer      *amqp.Channel

	handlers map[string]func([]byte) ([]byte, error)
	mu       *sync.Mutex
}

func NewServer(conn *amqp.Connection, RequestsQueue, ResponsesQueue string) (*RPCServer, error) {
	consumer, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("creating channel: %w", err)
	}

	producer, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("creating channel: %w", err)
	}

	rpc := &RPCServer{
		conn:           conn,
		consumer:       consumer,
		producer:       producer,
		RequestsQueue:  RequestsQueue,
		ResponsesQueue: ResponsesQueue,
		handlers:       make(map[string]func([]byte) ([]byte, error)),
		mu:             &sync.Mutex{},
	}

	return rpc, nil
}

// запуск прослушивания запросов
// errCh - канал в который будут записываться ошибки при работе воркера
func (r *RPCServer) Start(ctx context.Context, errCh chan<- error) error {
	deliveries, err := r.consumer.Consume(
		r.RequestsQueue,
		"", false, false, false, false, nil,
	)
	if err != nil {
		return fmt.Errorf("starting consume: %w", err)
	}

	go r.requestsListener(ctx, deliveries, errCh)

	return nil
}

// воркер, который будет принимать запросы и вызывать соответствующие методы
// errCh - сюда будут записываться ошибки
func (r *RPCServer) requestsListener(ctx context.Context, deliveries <-chan amqp.Delivery, errCh chan<- error) {

	publish := func(ctx context.Context, res []byte, corID string) {
		err := r.producer.PublishWithContext(
			ctx, // TODO: может тут стоит использовать контекст с таймаутом?
			"",
			r.ResponsesQueue,
			false,
			false,
			amqp.Publishing{
				ContentType:   "application/json",
				CorrelationId: corID,
				Body:          res,
			},
		)

		if err != nil {
			errCh <- fmt.Errorf("publish response: %w", err)
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case d, ok := <-deliveries:
			go func() {
				if !ok {
					errCh <- fmt.Errorf("consumer channel closed for queue %s", r.RequestsQueue)
					return
				}

				r.mu.Lock()
				handler, ok := r.handlers[d.Type]
				r.mu.Unlock()
				if !ok {
					err := fmt.Errorf("unidentified handler: %s", d.Type)
					errCh <- err
					publish(
						ctx,
						fmt.Appendf(nil, "{\n\terror: %s\n}", err),
						d.CorrelationId,
					)
					return
				}

				// если ReplyTo пусто, значит ответ не требуется
				if d.ReplyTo == "" {
					handler(d.Body)
					return
				}
				res, err := handler(d.Body)
				if err != nil {
					publish(
						ctx,
						fmt.Appendf(nil, "{\n\terror: %s\n}", err),
						d.CorrelationId,
					)
					return
				}

				publish(ctx, res, d.CorrelationId)
			}()
		}
	}
}

// установить новый обработчик
func (r *RPCServer) Handle(name string, handler func([]byte) ([]byte, error)) {
	r.mu.Lock() // мало ли
	r.handlers[name] = handler
	r.mu.Unlock()
}

// обертка над обработчиком, десериализует аргументы из JSON в T,
// вызывает собственно обработчик, помещает ответ в dto.RPCResponse и
// сериализует его в json
func HandlerWrap[T, B any](f func(T) (B, error)) func([]byte) ([]byte, error) {
	return func(raw []byte) ([]byte, error) {
		var arg T
		err := json.Unmarshal(raw, &arg)
		if err != nil {
			return nil, fmt.Errorf("umarshaling json arg: %w", err)
		}

		res, err := f(arg)

		rawRes, err := json.Marshal(res)
		if err != nil {
			return nil, fmt.Errorf("marshaling json res: %w", err)
		}

		response := dto.RPCResponse{
			Payload: rawRes,
			Err:     err,
		}

		rawResponse, err := json.Marshal(response)
		if err != nil {
			return nil, fmt.Errorf("marshaling json response: %w", err)
		}

		return rawResponse, nil
	}
}

// обертка над обработчиком, который не предполагает возврата значения,
// десериализует аргументы из JSON в T, вызывает собственно обработчик
func HandlerWrapNoRes[T any](f func(T) error) func([]byte) error {
	return func(raw []byte) error {
		var arg T
		err := json.Unmarshal(raw, &arg)
		if err != nil {
			return fmt.Errorf("umarshaling json arg: %w", err)
		}

		return f(arg)
	}
}
