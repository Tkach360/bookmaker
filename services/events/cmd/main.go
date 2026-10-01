package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"bookmaker/pkg/broker"
	"bookmaker/shared/dto"

	"github.com/joho/godotenv"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := godotenv.Load(); err != nil {
		log.Printf("could not find .env, using defaults")
	}

	brokerURL := os.Getenv("BROKER_URL")
	if brokerURL == "" {
		brokerURL = "amqp://guest:guest@localhost:5672/"
	}

	requestQueue := os.Getenv("EVENTS_REQUESTS_QUEUE")
	if requestQueue == "" {
		requestQueue = "events.requests"
	}

	responseQueue := os.Getenv("EVENTS_RESPONSES_QUEUE")
	if responseQueue == "" {
		responseQueue = "bets.events.responses"
	}

	var b *broker.Broker
	var err error

	maxRetries := 10
	for i := range maxRetries {
		b, err = broker.New(ctx, brokerURL)
		if err == nil {
			log.Println("Successfully connected to RabbitMQ")
			break
		}
		log.Printf("Failed to connect to RabbitMQ (attempt %d/%d): %v. Retrying in 2s...", i+1, maxRetries, err)
		time.Sleep(2 * time.Second)
	}

	if err != nil {
		log.Fatalf("Could not connect to RabbitMQ after %d attempts: %v", maxRetries, err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := b.Close(shutdownCtx); err != nil {
			log.Printf("broker close error: %v", err)
		}
	}()

	if err := b.ConnectConsumers(ctx, requestQueue); err != nil {
		log.Fatalf("connect consumers: %v", err)
	}
	if err := b.ConnectPublishers(ctx, responseQueue); err != nil {
		log.Fatalf("connect publishers: %v", err)
	}

	log.Printf(" [*] Events service is running. Waiting for messages on %q. Press CTRL+C to exit.", requestQueue)

	for {
		delivery, err := b.Receive(ctx, requestQueue)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				log.Println("Context canceled, shutting down events service")
				return
			}
			log.Printf("Failed to receive a message: %v. Retrying in 1s...", err)
			time.Sleep(1 * time.Second)
			continue
		}

		err = processRequest(ctx, delivery, b, responseQueue)
		if err != nil {
			log.Printf("Failed to process request: %v", err)
			_ = delivery.Nack(false, true)
			continue
		}

		_ = delivery.Ack(false)
	}
}

func processRequest(ctx context.Context, delivery amqp.Delivery, b *broker.Broker, responseQueue string) error {
	corID := delivery.CorrelationId

	var req dto.Request
	if err := json.Unmarshal(delivery.Body, &req); err != nil {
		log.Printf("invalid request format: %v", err)
		_ = delivery.Ack(false)
		return err
	}

	switch req.Action {
	case dto.GET_EVENT:
		handleGetEvent(ctx, delivery, req, corID, b, responseQueue)
	default:
		err := fmt.Errorf("unexpected action: %s", req.Action)
		log.Println(err)

		if corID != "" {
			bResp, _ := dto.PackResponse(nil, dto.ERROR, err)
			_ = b.PublishWithCorrelationId(ctx, responseQueue, corID, bResp)
		}
		_ = delivery.Ack(false)
		return err
	}

	return nil
}

func handleGetEvent(ctx context.Context, delivery amqp.Delivery, req dto.Request, corID string, b *broker.Broker, responseQueue string) {
	var payload map[string]int
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		log.Printf("failed to unmarshal get_event payload: %v", err)
		_ = delivery.Ack(false) // Discard
		return
	}
	eventID := payload["event_id"]

	// TODO: сейчас тут просто заглушка
	replyData := dto.EventStatusReply{
		EventID: eventID,
		Status:  dto.EVENT_OPEN,
	}

	responseBytes, err := dto.PackResponse(replyData, dto.OK, nil)
	if err != nil {
		log.Printf("failed to pack response: %v", err)
		_ = delivery.Nack(false, true) // Requeue
		return
	}

	if err := b.PublishWithCorrelationId(ctx, responseQueue, corID, responseBytes); err != nil {
		log.Printf("failed to send reply: %v", err)
		_ = delivery.Nack(false, true) // Requeue, так как не смогли отправить ответ
		return
	}

	_ = delivery.Ack(false)
}
