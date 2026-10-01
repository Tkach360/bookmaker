package broker

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Broker struct {
	conn       *amqp.Connection
	Consumers  map[string]<-chan amqp.Delivery
	Publishers map[string]*amqp.Channel
	pending    map[string]chan Result
	mu         *sync.Mutex
}

func New(ctx context.Context, uri string) (*Broker, error) {
	conn, err := amqp.Dial(uri)
	if err != nil {
		return nil, fmt.Errorf("dialing rabbitmq: %w", err)
	}

	return &Broker{
		conn:       conn,
		Consumers:  make(map[string]<-chan amqp.Delivery),
		Publishers: make(map[string]*amqp.Channel),
		pending:    make(map[string]chan Result),
		mu:         &sync.Mutex{},
	}, nil
}

func (b *Broker) Close(ctx context.Context) error {
	return b.conn.Close()
}

func (b *Broker) ConnectConsumers(ctx context.Context, qnames ...string) error {
	for _, qname := range qnames {
		if _, exists := b.Consumers[qname]; exists {
			continue
		}

		ch, err := b.conn.Channel()
		if err != nil {
			return fmt.Errorf("open channel for consumer %q: %w", qname, err)
		}

		args := amqp.Table{"x-queue-type": "quorum"}
		var declared bool
		for attempt := 1; attempt <= 5; attempt++ {
			_, err = ch.QueueDeclare(qname, true, false, false, false, args)
			if err == nil {
				declared = true
				break
			}
			log.Printf("QueueDeclare %q failed (attempt %d/5): %v. Retrying...", qname, attempt, err)
			time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
		}
		if !declared {
			ch.Close()
			return fmt.Errorf("declare queue %q after retries: %w", qname, err)
		}

		deliveries, err := ch.Consume(qname, "", false, false, false, false, nil)
		if err != nil {
			ch.Close()
			return fmt.Errorf("consuming from queue %q: %w", qname, err)
		}

		b.Consumers[qname] = deliveries
	}
	return nil
}

func (b *Broker) ConnectPublishers(ctx context.Context, qnames ...string) error {
	for _, qname := range qnames {
		if _, exists := b.Publishers[qname]; exists {
			continue
		}

		ch, err := b.conn.Channel()
		if err != nil {
			return fmt.Errorf("open channel for publisher %q: %w", qname, err)
		}

		args := amqp.Table{"x-queue-type": "quorum"}
		_, err = ch.QueueDeclare(qname, true, false, false, false, args)
		if err != nil {
			ch.Close()
			return fmt.Errorf("declare queue %q: %w", qname, err)
		}

		b.Publishers[qname] = ch
	}
	return nil
}

type Result struct {
	Body []byte
	Err  error
}

func (b *Broker) Receive(ctx context.Context, qname string) (amqp.Delivery, error) {
	deliveries, ok := b.Consumers[qname]
	if !ok {
		return amqp.Delivery{}, fmt.Errorf("consumer for queue %q is not connected", qname)
	}

	select {
	case <-ctx.Done():
		return amqp.Delivery{}, ctx.Err()
	case d, ok := <-deliveries:
		if !ok {
			return amqp.Delivery{}, fmt.Errorf("consumer channel closed for queue %q", qname)
		}
		return d, nil
	}
}

func (b *Broker) ResponceListener(ctx context.Context, qname string) error {
	for {
		d, err := b.Receive(ctx, qname)
		if err != nil {
			return fmt.Errorf("receiving msg: %w", err)
		}

		correlationID := d.CorrelationId

		b.mu.Lock()
		ch, exists := b.pending[correlationID]
		if exists {
			ch <- Result{
				Body: d.Body,
				Err:  nil,
			}
			delete(b.pending, correlationID)
		}
		b.mu.Unlock()
		d.Ack(false)
	}
}

func (b *Broker) Publish(ctx context.Context, qname, replyQueue string, data []byte) (<-chan Result, error) {
	ch, ok := b.Publishers[qname]
	if !ok {
		return nil, fmt.Errorf("publisher for queue %q not found", qname)
	}

	correlationID := uuid.New().String()

	msg := amqp.Publishing{
		ContentType:   "application/json",
		CorrelationId: correlationID,
		ReplyTo:       replyQueue,
		Body:          data,
	}

	err := ch.PublishWithContext(ctx, "", qname, false, false, msg)
	if err != nil {
		return nil, fmt.Errorf("publishing message: %w", err)
	}

	respCh := make(chan Result, 1)
	b.mu.Lock()
	b.pending[correlationID] = respCh
	b.mu.Unlock()

	return respCh, nil
}

func (b *Broker) PublishWithCorrelationId(ctx context.Context, qname, correlationID string, data []byte) error {
	ch, ok := b.Publishers[qname]
	if !ok {
		return fmt.Errorf("publisher for queue %q not found", qname)
	}

	msg := amqp.Publishing{
		ContentType:   "application/json",
		CorrelationId: correlationID,
		Body:          data,
	}

	err := ch.PublishWithContext(ctx, "", qname, false, false, msg)
	if err != nil {
		return fmt.Errorf("publishing message: %w", err)
	}

	return nil
}
