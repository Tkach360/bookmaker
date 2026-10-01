package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	gateway "bookmaker/gateway/internal"
	"bookmaker/pkg/broker"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
)

func main() {

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := godotenv.Load(); err != nil {
		log.Printf("could not find .env, using default")
	}
	brokerURL := os.Getenv("BROKER_URL")
	if brokerURL == "" {
		brokerURL = "amqp://guest:guest@localhost:5672/"
	}

	requestQueue := os.Getenv("GATEWAY_REQUESTS_BETS")
	if requestQueue == "" {
		requestQueue = "bets.requests"
	}
	responceQueue := os.Getenv("GATEWAY_RESPONSES_BETS")
	if responceQueue == "" {
		responceQueue = "bets.responses"
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

	r := gin.Default()
	betClient := gateway.NewBetClient(ctx, b, requestQueue, responceQueue)
	gateway := gateway.Gateway{
		Router:    r,
		BetClient: betClient,
	}
	gateway.SetupRouter()

	srv := &http.Server{
		Addr:    ":8080",
		Handler: r,
	}

	go func() {
		log.Println("Server started on :8080")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("Shutting down gracefully...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("Server forced to shutdown: %v", err)
	}
	log.Println("Server exited")
}
