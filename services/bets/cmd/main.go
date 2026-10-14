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

	"bookmaker/bets/internal"
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

	// запросы для bets
	betsRequestsQueue := getenv("BETS_REQUESTS_QUEUE", "bets.requests")
	// запросы для events
	eventsRequestsQueue := getenv("EVENTS_REQUESTS_QUEUE", "events.requests")
	// ответы на запросы, которые bets отправляет другим сервисам;
	// пусто - имя очереди выдаст брокер
	betsResponsesQueue := getenv("BETS_RESPONSES_QUEUE", "")

	s := store.NewMapStore[dto.Bet]()
	s.Put(1, dto.Bet{Id: 1, EventId: 1, Amount: decimal.NewFromFloat(1000.0), User: "Вячеслав"})
	s.Put(2, dto.Bet{Id: 2, EventId: 1, Amount: decimal.NewFromFloat(250.50), User: "Анна"})
	s.Put(3, dto.Bet{Id: 3, EventId: 2, Amount: decimal.NewFromFloat(15000.00), User: "Дмитрий"})
	s.Put(4, dto.Bet{Id: 4, EventId: 2, Amount: decimal.NewFromFloat(75.25), User: "Мария"})
	s.Put(5, dto.Bet{Id: 5, EventId: 3, Amount: decimal.NewFromFloat(4999.99), User: "Игорь"})
	s.Put(6, dto.Bet{Id: 6, EventId: 3, Amount: decimal.NewFromFloat(12345.67), User: "Ольга"})

	conn, err := rmq.DialWithRetry(ctx, brokerURL, 15, 2*time.Second)
	if err != nil {
		return fmt.Errorf("dialing rabbitmq: %w", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("broker close error: %v", err)
		}
	}()

	betService, err := internal.NewBetService(s, conn, betsRequestsQueue, eventsRequestsQueue, betsResponsesQueue)
	if err != nil {
		return fmt.Errorf("creating bet service: %w", err)
	}
	// defer'ы идут в обратном порядке: сначала Close сервиса
	// (дожидается обработчиков), потом закрытие соединения
	defer func() {
		if err := betService.Close(); err != nil {
			log.Printf("bet service close error: %v", err)
		}
	}()

	// канал намеренно не закрываем: в него могут писать до самого Close
	errCh := make(chan error, 16)
	go func() {
		for err := range errCh {
			log.Printf("bet service error: %v", err)
		}
	}()

	if err := betService.Start(ctx, errCh); err != nil {
		return fmt.Errorf("starting bet service: %w", err)
	}

	log.Printf(" [*] Bets service is running. Waiting for requests on %q. Press CTRL+C to exit.", betsRequestsQueue)

	<-ctx.Done()
	log.Println("Shutting down bet service...")

	return nil
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
