package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"bookmaker/bets"
	gateway "bookmaker/gateway/internal"
	"bookmaker/servers"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
)

func main() {
	if err := run(); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

// Вся логика находится здесь, чтобы defer'ы выполнялись (log.Fatal в main их пропускает).
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
		return err
	}
	defer closeLogged("rabbitmq connection", rabbitmq)

	// Канал для запросов к сервису Bets.
	betsRequests, err := rabbitmq.OpenBetsRequestsAMQP(ctx, nil)
	if err != nil {
		return fmt.Errorf("opening bets requests channel: %w", err)
	}
	defer closeLogged("bets requests channel", betsRequests)

	// Канал для запросов к сервису Events.
	eventsRequests, err := rabbitmq.OpenEventsRequestsAMQP(ctx, nil)
	if err != nil {
		return fmt.Errorf("opening events requests channel: %w", err)
	}
	defer closeLogged("events requests channel", eventsRequests)

	// Общая очередь ответов для Gateway.
	gatewayResponses, err := rabbitmq.OpenGatewayResponsesAMQP(ctx, nil)
	if err != nil {
		return fmt.Errorf("opening gateway responses channel: %w", err)
	}
	defer closeLogged("gateway responses channel", gatewayResponses)

	betClient := bets.NewBetClient(
		betsRequests,
		eventsRequests,
		gatewayResponses,
		func(err error) {
			log.Printf("bet client error: %v", err)
		},
	)

	// Клиент работает до завершения HTTP shutdown, чтобы успеть получить ответы
	// на запросы, которые уже обрабатываются.
	clientCtx, stopClient := context.WithCancel(context.Background())
	clientDone := make(chan struct{})
	go func() {
		defer close(clientDone)
		if err := betClient.Run(clientCtx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("bet client stopped: %v", err)
		}
	}()
	defer func() {
		stopClient()
		<-clientDone
	}()

	r := gin.Default()
	gw := gateway.NewGateway(r, betClient)
	gw.SetupRouter()

	srv := &http.Server{
		Addr:    ":8080",
		Handler: r,
	}

	srvErr := make(chan error, 1)
	go func() {
		log.Println("Server started on :8080")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			srvErr <- err
		}
	}()

	select {
	case <-ctx.Done():
		log.Println("Shutting down gracefully...")
	case err := <-srvErr:
		return fmt.Errorf("server error: %w", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("Server forced to shutdown: %v", err)
	}

	log.Println("Server exited")
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
