package rmq

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Result struct {
	Body []byte
	Err  error
}

type RPCClient struct {
	conn *amqp.Connection

	ResponsesQueue string // очередь RabbitMQ, в которой будут ответы
	consumer       *amqp.Channel

	RequestsQueue string // очередь RabbitMQ, в которую будем помещать запросы
	producer      *amqp.Channel

	// мапа для каналов ожидания ответа
	// correlationID - ключ, канал с ответом - значение
	pending map[string]chan []byte
	mu      *sync.Mutex
}

func NewClient(conn *amqp.Connection, RequestsQueue, ResponsesQueue string) (*RPCClient, error) {
	consumer, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("creating channel: %w", err)
	}

	producer, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("creating channel: %w", err)
	}

	rpc := &RPCClient{
		conn:           conn,
		consumer:       consumer,
		producer:       producer,
		RequestsQueue:  RequestsQueue,
		ResponsesQueue: ResponsesQueue,
		pending:        make(map[string]chan []byte),
		mu:             &sync.Mutex{},
	}

	return rpc, nil
}

// запуск прослушивания ответов
// errCh - канал в который будут записываться ошибки при работе воркера
func (r *RPCClient) Start(ctx context.Context, errCh chan<- error) error {
	deliveries, err := r.consumer.Consume(
		r.ResponsesQueue,
		"", false, false, false, false, nil,
	)
	if err != nil {
		return fmt.Errorf("starting consume: %w", err)
	}

	go r.responseListener(ctx, deliveries, errCh)

	return nil
}

// отправить сообщение без ожидания ответа (отправил и забыл)
// mname - имя вызываемого метода
// args - аргументы
func (r *RPCClient) Send(ctx context.Context, mname string, payload json.RawMessage) error {

	corID := uuid.New().String()

	err := r.consumer.PublishWithContext(
		ctx,
		"",
		r.RequestsQueue,
		false,
		false,
		amqp.Publishing{
			Type:          mname,
			ContentType:   "application/json",
			CorrelationId: corID,
			ReplyTo:       "", // пусто потому что не предполагаем ответа
			Body:          payload,
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
// - args - аргументы
func (r *RPCClient) Call(ctx context.Context, mname string, payload json.RawMessage) (error, <-chan []byte) {
	corID := uuid.New().String()

	err := r.consumer.PublishWithContext(
		ctx,
		"",
		r.RequestsQueue,
		false,
		false,
		amqp.Publishing{
			Type:          mname,
			ContentType:   "application/json",
			CorrelationId: corID,
			ReplyTo:       r.ResponsesQueue,
			Body:          payload,
		},
	)

	if err != nil {
		return fmt.Errorf("publishing message: %w", err), nil
	}

	respCh := make(chan []byte, 1)

	r.mu.Lock()
	r.pending[corID] = respCh
	r.mu.Unlock()

	return nil, respCh
}

// воркер, который читает ответы от других микросервисов и помещает их в соответствующие каналы
// ошибки записывает в errCh
func (r *RPCClient) responseListener(ctx context.Context, deliveries <-chan amqp.Delivery, errCh chan<- error) {
	for {
		select {
		case <-ctx.Done():
			errCh <- ctx.Err()
			return
		case d, ok := <-deliveries:
			if !ok {
				errCh <- fmt.Errorf("consumer channel closed for queue %s", r.ResponsesQueue)
				// return?
				continue
			}

			corID := d.CorrelationId

			r.mu.Lock()
			ch, exists := r.pending[corID]
			if exists {
				ch <- d.Body
			}
			r.mu.Unlock()
			d.Ack(false)
		}
	}
}
