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
	"bookmaker/pkg/rmq"

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

	requestsQueue := os.Getenv("GATEWAY_REQUESTS_BETS")
	if requestsQueue == "" {
		requestsQueue = "bets.requests"
	}
	responsesQueue := os.Getenv("GATEWAY_RESPONSES_BETS")
	if responsesQueue == "" {
		responsesQueue = "bets.responses"
	}

	conn, err := rmq.DialWithRetry(ctx, brokerURL, 10, 2*time.Second)
	if err != nil {
		log.Fatal("dialing rabbitmq: " + err.Error())
	}

	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("broker close error: %v", err)
		}
	}()

	r := gin.Default()
	betClient, err := gateway.NewBetClient(conn, requestsQueue, responsesQueue)
	if err != nil {
		log.Fatal(err)
		return
	}
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
