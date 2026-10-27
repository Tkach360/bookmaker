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

	"bookmaker/bets/internal"
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

// Вся логика находится в run, чтобы defer'ы выполнялись до возврата в main.
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

	rabbitmq, err := connectWithRetry(ctx, brokerURL, 15, 2*time.Second, servers.ConnectRabbitmqBidi)
	if err != nil {
		return fmt.Errorf("connecting to RabbitMQ: %w", err)
	}
	defer closeLogged("rabbitmq connection", rabbitmq)

	// Имена очередей берутся из AsyncAPI-контракта.
	betsRequests, err := rabbitmq.OpenBetsRequestsAMQP(ctx, nil)
	if err != nil {
		return fmt.Errorf("opening bets requests channel: %w", err)
	}
	defer closeLogged("bets requests channel", betsRequests)

	eventsRequests, err := rabbitmq.OpenEventsRequestsAMQP(ctx, nil)
	if err != nil {
		return fmt.Errorf("opening events requests channel: %w", err)
	}
	defer closeLogged("events requests channel", eventsRequests)

	betsResponses, err := rabbitmq.OpenBetsResponsesAMQP(ctx, nil)
	if err != nil {
		return fmt.Errorf("opening bets responses channel: %w", err)
	}
	defer closeLogged("bets responses channel", betsResponses)

	gatewayResponses, err := rabbitmq.OpenGatewayResponsesAMQP(ctx, nil)
	if err != nil {
		return fmt.Errorf("opening gateway responses channel: %w", err)
	}
	defer closeLogged("gateway responses channel", gatewayResponses)

	// Заглушечное хранилище ставок, как и в прежней реализации.
	st := store.NewMapStore[dto.Bet]()
	st.Put(1, dto.Bet{Id: 1, EventId: 1, Amount: decimal.NewFromFloat(1000.0), User: "Вячеслав"})
	st.Put(2, dto.Bet{Id: 2, EventId: 1, Amount: decimal.NewFromFloat(250.50), User: "Анна"})
	st.Put(3, dto.Bet{Id: 3, EventId: 2, Amount: decimal.NewFromFloat(15000.00), User: "Дмитрий"})
	st.Put(4, dto.Bet{Id: 4, EventId: 2, Amount: decimal.NewFromFloat(75.25), User: "Мария"})
	st.Put(5, dto.Bet{Id: 5, EventId: 3, Amount: decimal.NewFromFloat(4999.99), User: "Игорь"})
	st.Put(6, dto.Bet{Id: 6, EventId: 3, Amount: decimal.NewFromFloat(12345.67), User: "Ольга"})

	betService := internal.NewBetService(
		st,
		betsRequests,
		eventsRequests,
		betsResponses,
		gatewayResponses,
		func(err error) { log.Printf("bets service error: %v", err) },
	)
	// Сначала останавливается сервис и дожидается его фоновых циклов,
	// затем закрываются каналы и соединение RabbitMQ.
	defer closeLogged("bet service", betService)

	errCh := make(chan error, 16)
	go func() {
		for err := range errCh {
			log.Printf("bet service error: %v", err)
		}
	}()

	if err := betService.Start(ctx, errCh); err != nil {
		return fmt.Errorf("starting bet service: %w", err)
	}

	log.Printf(" [*] Bets service is running. Waiting for requests on %q. Press CTRL+C to exit.", betsRequests.Queue())
	<-ctx.Done()
	log.Println("Shutting down bet service...")
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
