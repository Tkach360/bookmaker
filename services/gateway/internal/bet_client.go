package gateway

import (
	"bookmaker/pkg/rmq"
	"bookmaker/shared/dto"
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/shopspring/decimal"
)

type BetClient struct {
	client         *rmq.RPCClient
	RequestQueue   string
	ResponsesQueue string
}

func NewBetClient(conn *amqp.Connection, requestsQueue, responsesQueue string) (*BetClient, error) {

	client, err := rmq.NewClient(conn, responsesQueue)
	if err != nil {
		return nil, fmt.Errorf("creating rpc client: %w", err)
	}

	return &BetClient{
		client:         client,
		RequestQueue:   requestsQueue,
		ResponsesQueue: responsesQueue,
	}, nil
}

func (b *BetClient) Start(ctx context.Context, errCh chan<- error) error {
	return b.client.Start(ctx, errCh)
}

// получить одну ставку
func (b *BetClient) GetBet(ctx context.Context, id int) (*dto.Bet, rmq.RPCResponseStatus, error) {
	return rmq.Call[dto.GetBetRequest, dto.Bet](ctx, b.client, dto.GET_BET, dto.GetBetRequest{Id: id})
}

func (b *BetClient) CreateBet(ctx context.Context, userID, eventID int, amount decimal.Decimal) (*dto.Bet, rmq.RPCResponseStatus, error) {
	return rmq.Call[dto.CreateBetRequest, dto.Bet](ctx, b.client, dto.CREATE_BET, dto.CreateBetRequest{
		UserID:  userID,
		EventID: eventID,
		Amount:  amount,
	}) // TODO: стоит вынести формирование запрос на создание наружу
}
