package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	events "bookmaker/events/internal"
	"bookmaker/pkg/rmq"
	"bookmaker/pkg/store"
	"bookmaker/shared/dto"

	"github.com/joho/godotenv"
	"github.com/shopspring/decimal"
)

func main() {
	if err := run(); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

// вся логика здесь, чтобы defer'ы выполнялись (log.Fatal в main их пропускает)
func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := godotenv.Load(); err != nil {
		log.Printf("could not find .env, using defaults")
	}

	brokerURL := getenv("BROKER_URL", "amqp://guest:guest@localhost:5672/")
	requestsQueue := getenv("EVENTS_REQUESTS_QUEUE", "events.requests")

	conn, err := rmq.DialWithRetry(ctx, brokerURL, 15, 2*time.Second)
	if err != nil {
		return err
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("broker close error: %v", err)
		}
	}()

	server, err := rmq.NewServer(conn, requestsQueue)
	if err != nil {
		return fmt.Errorf("creating rpc server: %w", err)
	}
	// defer'ы идут в обратном порядке: сначала Close сервера
	// (дожидается обработчиков), потом закрытие соединения
	defer func() {
		if err := server.Close(); err != nil {
			log.Printf("rpc server close error: %v", err)
		}
	}()

	// канал намеренно не закрываем: в него могут писать до самого Close
	errCh := make(chan error, 16)
	go func() {
		for err := range errCh {
			log.Printf("events service error: %v", err)
		}
	}()

	store := store.NewMapStore[dto.Event]()
	store.Put(1, dto.Event{Id: 1, Coefficient: decimal.NewFromFloat(0.6), IsWin: false, IsTook: false})
	store.Put(2, dto.Event{Id: 2, Coefficient: decimal.NewFromFloat(0.2), IsWin: true, IsTook: true})
	store.Put(3, dto.Event{Id: 3, Coefficient: decimal.NewFromFloat(0.9), IsWin: false, IsTook: true})

	eventService := events.EventService{
		Server: server,
		Store:  store,
	}
	if err := eventService.Start(ctx, errCh); err != nil {
		return fmt.Errorf("starting events service: %w", err)
	}

	log.Printf(" [*] Events service is running. Waiting for requests on %q. Press CTRL+C to exit.", requestsQueue)

	<-ctx.Done()
	log.Println("Shutting down events service...")

	return nil
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
