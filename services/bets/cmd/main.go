package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"bookmaker/pkg/rmq"
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

	requestsQueue := os.Getenv("BETS_REQUESTS_QUEUE")
	if requestsQueue == "" {
		requestsQueue = "bets.requests"
	}

	responsesQueue := os.Getenv("BETS_RESPONSES_QUEUE")
	if responsesQueue == "" {
		responsesQueue = "bets.responses"
	}

	s := store.NewMapStore[dto.Bet]()
	s.Put(1, dto.Bet{Id: 1, EventId: 1, Amount: decimal.NewFromFloat(1000.0), User: "Вячеслав"})
	s.Put(2, dto.Bet{Id: 2, EventId: 1, Amount: decimal.NewFromFloat(250.50), User: "Анна"})
	s.Put(3, dto.Bet{Id: 3, EventId: 2, Amount: decimal.NewFromFloat(15000.00), User: "Дмитрий"})
	s.Put(4, dto.Bet{Id: 4, EventId: 2, Amount: decimal.NewFromFloat(75.25), User: "Мария"})
	s.Put(5, dto.Bet{Id: 5, EventId: 3, Amount: decimal.NewFromFloat(4999.99), User: "Игорь"})
	s.Put(6, dto.Bet{Id: 6, EventId: 3, Amount: decimal.NewFromFloat(12345.67), User: "Ольга"})

	conn, err := rmq.DialWithRetry(ctx, brokerURL, 10, 2*time.Second)
	if err != nil {
		log.Fatal("dialing rabbitmq: " + err.Error())
	}

	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("broker close error: %v", err)
		}
	}()

	eventsRequestQueue := "events.requests"
	eventsResponseQueue := "bets.events.responses"

	client, err := rmq.NewClient(conn, eventsResponseQueue)
	if err != nil {
		log.Fatal(err)
		return
	}
	eventClient := internal.NewEventClient(client, eventsRequestQueue)
	server, err := rmq.NewServer(conn, requestsQueue)
	if err != nil {
		log.Fatalf("broker close error: %v", err)
		return
	}

	betService := internal.BetService{
		Store:       s,
		Server:      server,
		EventClient: eventClient,
	}

	errCh := make(chan error)
	go func() {
		select {
		case err := <-errCh:
			log.Print(err)
		default:
		}
	}()

	betService.Start(ctx, errCh)
	<-ctx.Done()
	log.Println("Shutting down bet service...")
}
