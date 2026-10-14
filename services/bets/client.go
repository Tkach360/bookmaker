package bets

import (
	"context"

	"bookmaker/pkg/rmq"
	"bookmaker/shared/dto"

	"github.com/shopspring/decimal"
)

// BetClient - типизированная обёртка над общим RPCClient микросервиса
// для вызовов сервиса bets
// Сам RPCClient (очередь ответов, Start/Close) создаtт и закрывает владелец,
// а не BetClient
type BetClient struct {
	rpc *rmq.RPCClient
}

// NewBetClient регистрирует методы сервиса bets в общем RPCClient.
// requestQueue - очередь запросов сервиса bets.
func NewBetClient(rpc *rmq.RPCClient, requestQueue string) *BetClient {
	rpc.AddMethod(dto.GET_BET, requestQueue)
	rpc.AddMethod(dto.CREATE_BET, requestQueue)

	return &BetClient{rpc: rpc}
}

// получить одну ставку
func (b *BetClient) GetBet(ctx context.Context, id int) (*dto.Bet, rmq.RPCResponseStatus, error) {
	return rmq.Call[dto.GetBetRequest, dto.Bet](ctx, b.rpc, dto.GET_BET, dto.GetBetRequest{Id: id})
}

// создать ставку
func (b *BetClient) CreateBet(ctx context.Context, userID, eventID int, amount decimal.Decimal) (*dto.Bet, rmq.RPCResponseStatus, error) {
	return rmq.Call[dto.CreateBetRequest, dto.Bet](ctx, b.rpc, dto.CREATE_BET, dto.CreateBetRequest{
		UserID:  userID,
		EventID: eventID,
		Amount:  amount,
	}) // TODO: стоит вынести формирование запроса на создание наружу
}
