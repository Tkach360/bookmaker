package rmq

import (
	"context"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// подключается к RabbitMQ с повторными попытками
// ожидание между попытками прерывается отменой ctx
func DialWithRetry(ctx context.Context, url string, attempts int, delay time.Duration) (*amqp.Connection, error) {
	var lastErr error

	for i := 1; i <= attempts; i++ {
		conn, err := amqp.Dial(url)
		if err == nil {
			log.Println("Successfully connected to RabbitMQ")
			return conn, nil
		}

		lastErr = err
		log.Printf("Failed to connect to RabbitMQ (attempt %d/%d): %v", i, attempts, err)

		if i == attempts {
			break
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}

	return nil, fmt.Errorf("could not connect to RabbitMQ after %d attempts: %w", attempts, lastErr)
}
