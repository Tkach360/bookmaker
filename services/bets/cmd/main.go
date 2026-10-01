package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"bookmaker/pkg/broker"
	"bookmaker/pkg/store"
	"bookmaker/services/bets/internal"
	"bookmaker/shared/dto"

	"github.com/joho/godotenv"
	"github.com/shopspring/decimal"
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

	requestQueue := os.Getenv("BETS_REQUESTS_QUEUE")
	if requestQueue == "" {
		requestQueue = "bets.requests"
	}

	responseQueue := os.Getenv("BETS_RESPONSES_QUEUE")
	if responseQueue == "" {
		responseQueue = "bets.responses"
	}

	s := store.NewMapStore[dto.Bet]()
	s.Put(1, dto.Bet{Id: 1, EventId: 1, Amount: decimal.NewFromFloat(1000.0), User: "Вячеслав"})
	s.Put(2, dto.Bet{Id: 2, EventId: 1, Amount: decimal.NewFromFloat(250.50), User: "Анна"})
	s.Put(3, dto.Bet{Id: 3, EventId: 2, Amount: decimal.NewFromFloat(15000.00), User: "Дмитрий"})
	s.Put(4, dto.Bet{Id: 4, EventId: 2, Amount: decimal.NewFromFloat(75.25), User: "Мария"})
	s.Put(5, dto.Bet{Id: 5, EventId: 3, Amount: decimal.NewFromFloat(4999.99), User: "Игорь"})
	s.Put(6, dto.Bet{Id: 6, EventId: 3, Amount: decimal.NewFromFloat(12345.67), User: "Ольга"})

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
		log.Fatalf("❌ Could not connect to RabbitMQ after %d attempts: %v", maxRetries, err)
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

	eventsRequestQueue := "events.requests"
	eventsResponseQueue := "bets.events.responses"

	eventClient := internal.NewEventClient(ctx, b, eventsRequestQueue, eventsResponseQueue)

	betService := internal.BetService{
		Store:         s,
		Broker:        b,
		RequestQueue:  requestQueue,
		ResponseQueue: responseQueue,
		EventClient:   eventClient,
	}

	betService.Run(ctx)
	<-ctx.Done()
	log.Println("Shutting down bet service...")
}
