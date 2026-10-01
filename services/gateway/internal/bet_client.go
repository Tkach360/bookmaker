package gateway

import (
	"bookmaker/pkg/broker"
	"bookmaker/pkg/store"
	"bookmaker/shared/dto"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/shopspring/decimal"
)

type BetClient struct {
	Broker       *broker.Broker
	RequestQueue string
	ReplyQueue   string
}

func NewBetClient(ctx context.Context, broker *broker.Broker, requestQueue, replyQueue string) *BetClient {
	if err := broker.ConnectPublishers(ctx, requestQueue); err != nil {
		log.Fatalf("connect publisher: %v", err)
	}

	if err := broker.ConnectConsumers(ctx, replyQueue); err != nil {
		log.Fatalf("connect consumer: %v", err)
	}

	go func() {
		if err := broker.ResponceListener(ctx, replyQueue); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("Response listener exited with error: %v", err)
		}
	}()

	return &BetClient{
		Broker:       broker,
		RequestQueue: requestQueue,
		ReplyQueue:   replyQueue,
	}
}

// получить одну ставку
func (b *BetClient) GetBet(ctx context.Context, id int) (*dto.Bet, error) {
	request, err := dto.PackRequest(dto.GET_BET, dto.GetBetRequest{Id: id})
	if err != nil {
		return nil, fmt.Errorf("get bet: %w", err)
	}

	respCh, err := b.Broker.Publish(ctx, b.RequestQueue, b.ReplyQueue, request)
	if err != nil {
		return nil, fmt.Errorf("publish msg: %w", err)
	}

	select {
	case resp := <-respCh:
		if resp.Err != nil {
			return nil, fmt.Errorf("rpc error: %w", resp.Err)
		}

		var reply dto.Response
		if err := json.Unmarshal(resp.Body, &reply); err != nil {
			return nil, fmt.Errorf("unmarshaling reply json: %w", err)
		}

		if reply.Status == dto.NOT_FOUND {
			return nil, store.ErrNotFound
		}
		if reply.Status == dto.ERROR {
			return nil, fmt.Errorf("bet service error: %s", reply.Error)
		}
		if reply.Status == dto.OK && reply.Payload != nil {
			var bet dto.Bet
			err := json.Unmarshal(reply.Payload, &bet)
			if err != nil {
				return nil, fmt.Errorf("unmarshaling bet: %w", err)
			}
			return &bet, nil
		}

		return nil, fmt.Errorf("unexpected reply status: %s", reply.Status)

	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (b *BetClient) CreateBet(ctx context.Context, userID, eventID int, amount decimal.Decimal) (*dto.Bet, error) {
	reqData := dto.CreateBetRequest{
		UserID:  userID,
		EventID: eventID,
		Amount:  amount,
	}

	requestBytes, err := dto.PackRequest(dto.CREATE_BET, reqData)
	if err != nil {
		return nil, fmt.Errorf("pack create bet request: %w", err)
	}

	respCh, err := b.Broker.Publish(ctx, b.RequestQueue, b.ReplyQueue, requestBytes)
	if err != nil {
		return nil, fmt.Errorf("publish msg: %w", err)
	}

	select {
	case resp := <-respCh:
		if resp.Err != nil {
			return nil, fmt.Errorf("rpc error: %w", resp.Err)
		}

		var reply dto.Response
		if err := json.Unmarshal(resp.Body, &reply); err != nil {
			return nil, fmt.Errorf("unmarshaling reply json: %w", err)
		}

		if reply.Status == dto.ERROR {
			if reply.Error == "event is closed or finished" {
				return nil, fmt.Errorf("event_closed: %w", errors.New(reply.Error))
			}
			return nil, fmt.Errorf("bet service error: %s", reply.Error)
		}

		if reply.Status == dto.OK && reply.Payload != nil {
			var bet dto.Bet
			if err := json.Unmarshal(reply.Payload, &bet); err != nil {
				return nil, fmt.Errorf("unmarshaling bet: %w", err)
			}
			return &bet, nil
		}

		return nil, fmt.Errorf("unexpected reply status: %s", reply.Status)

	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
