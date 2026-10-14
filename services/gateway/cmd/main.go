package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"bookmaker/bets"
	gateway "bookmaker/gateway/internal"
	"bookmaker/pkg/rmq"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
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
	// очередь запросов сервиса bets
	betsRequestsQueue := getenv("GATEWAY_REQUESTS_BETS", "bets.requests")
	// очередь ответов на запросы gateway; пусто - имя выдаст брокер
	responsesQueue := getenv("GATEWAY_RESPONSES_BETS", "")

	conn, err := rmq.DialWithRetry(ctx, brokerURL, 15, 2*time.Second)
	if err != nil {
		return fmt.Errorf("dialing rabbitmq: %w", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("broker close error: %v", err)
		}
	}()

	client, err := rmq.NewClient(conn, responsesQueue)
	if err != nil {
		return fmt.Errorf("creating rpc client: %w", err)
	}
	// defer'ы идут в обратном порядке: клиент закрывается после остановки
	// HTTP-сервера (он в теле функции) и до закрытия соединения
	defer func() {
		if err := client.Close(); err != nil {
			log.Printf("rpc client close error: %v", err)
		}
	}()

	// канал намеренно не закрываем: в него могут писать до самого Close
	errCh := make(chan error, 16)
	go func() {
		for err := range errCh {
			log.Printf("gateway error: %v", err)
		}
	}()

	// клиент запускаем с отдельным контекстом, а не с сигнальным: при SIGTERM
	// HTTP-сервер должен успеть дообработать текущие запросы, а клиент им нужен
	if err := client.Start(context.Background(), errCh); err != nil {
		return fmt.Errorf("starting rpc client: %w", err)
	}

	betClient := bets.NewBetClient(client, betsRequestsQueue)

	r := gin.Default()
	gw := gateway.Gateway{
		Router:    r,
		BetClient: betClient,
	}
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

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
