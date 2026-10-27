package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	events "bookmaker/events/internal"
	"bookmaker/pkg/store"
	"bookmaker/servers"
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

	brokerURL, err := url.Parse(getenv("BROKER_URL", "amqp://guest:guest@localhost:5672/"))
	if err != nil {
		return fmt.Errorf("parsing BROKER_URL: %w", err)
	}

	// отдельные соединения для чтения запросов и отправки ответов
	rabbitmq, err := connectWithRetry(ctx, brokerURL, 15, 2*time.Second, servers.ConnectRabbitmqBidi)
	if err != nil {
		return err
	}
	defer closeLogged("rabbitmq connection", rabbitmq)

	// producer, err := connectWithRetry(ctx, brokerURL, 15, 2*time.Second, servers.ConnectRabbitmqProducer)
	// if err != nil {
	// 	return err
	// }
	// defer closeLogged("producer connection", producer)

	// открываю очереди для общения с остальными микросервисами
	requests, err := rabbitmq.OpenEventsRequestsAMQP(ctx, nil)
	if err != nil {
		return fmt.Errorf("opening events requests channel: %w", err)
	}
	betsResponses, err := rabbitmq.OpenBetsResponsesAMQP(ctx, nil)
	if err != nil {
		return fmt.Errorf("opening bets responses channel: %w", err)
	}
	gatewayResponses, err := rabbitmq.OpenGatewayResponsesAMQP(ctx, nil)
	if err != nil {
		return fmt.Errorf("opening gateway responses channel: %w", err)
	}

	// инициализирую хранилище данных
	st := store.NewMapStore[dto.Event]()
	st.Put(1, dto.Event{Id: 1, Coefficient: decimal.NewFromFloat(0.6), IsWin: false, IsTook: false})
	st.Put(2, dto.Event{Id: 2, Coefficient: decimal.NewFromFloat(0.2), IsWin: true, IsTook: true})
	st.Put(3, dto.Event{Id: 3, Coefficient: decimal.NewFromFloat(0.9), IsWin: false, IsTook: true})

	// создаю сервер
	server := events.NewServer(
		events.NewEventService(st),
		requests, betsResponses, gatewayResponses,
		func(err error) { log.Printf("events service error: %v", err) },
	)

	log.Printf(" [*] Events service is running. Waiting for requests on %q. Press CTRL+C to exit.", requests.Queue())

	// Run возвращается при отмене ctx; если же подписка закрылась сама
	// (обрыв соединения), ctx жив - это ошибка, пусть контейнер перезапустится
	if err := server.Run(ctx); err != nil {
		return fmt.Errorf("events server: %w", err)
	}
	if ctx.Err() == nil {
		return errors.New("events server stopped unexpectedly: consumer closed")
	}

	log.Println("Shutting down events service...")
	return nil
}

func connectWithRetry(
	ctx context.Context,
	u *url.URL,
	attempts int,
	delay time.Duration,
	connect func(context.Context, *url.URL) (*servers.RabbitmqClosable, error),
) (*servers.RabbitmqClosable, error) {
	var lastErr error

	for i := 1; i <= attempts; i++ {
		conn, err := connect(ctx, u)
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

func closeLogged(name string, c io.Closer) {
	if err := c.Close(); err != nil {
		log.Printf("%s close error: %v", name, err)
	}
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
