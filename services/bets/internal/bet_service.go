package internal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"bookmaker/pkg/broker"
	"bookmaker/pkg/store"
	"bookmaker/shared/dto"

	amqp "github.com/rabbitmq/amqp091-go"
)

type BetService struct {
	Store         store.Store[dto.Bet]
	Broker        *broker.Broker
	RequestQueue  string
	ResponseQueue string
	EventClient   *EventClient
}

func (b *BetService) Run(ctx context.Context) {
	go func() {
		for {
			delivery, err := b.Broker.Receive(ctx, b.RequestQueue)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					log.Println("Context canceled, stopping consumer")
					return
				}
				log.Printf("Failed to receive a message: %v. Retrying in 1s...", err)
				time.Sleep(1 * time.Second)
				continue
			}

			err = b.ProcessRequest(ctx, delivery)
			if err != nil {
				log.Printf("Unexpected error processing request: %v. Requeuing...", err)
				_ = delivery.Nack(false, true)
				continue
			}
		}
	}()
}

func (b *BetService) discardAndError(ctx context.Context, delivery amqp.Delivery, e error) {
	_ = delivery.Ack(false)

	bResp, packErr := dto.PackResponse(nil, dto.ERROR, e)
	if packErr != nil {
		bResp = fmt.Appendf(nil, `{"status":"error","error":"%s"}`, e.Error())
	}

	corID := delivery.CorrelationId

	if corID != "" {
		_ = b.Broker.PublishWithCorrelationId(ctx, b.ResponseQueue, corID, bResp)
	}
}

func (b *BetService) ProcessRequest(ctx context.Context, delivery amqp.Delivery) error {
	corID := delivery.CorrelationId

	var req dto.Request
	if err := json.Unmarshal(delivery.Body, &req); err != nil {
		b.discardAndError(ctx, delivery, fmt.Errorf("invalid request format: %w", err))
		return nil
	}

	switch req.Action {
	case dto.GET_BET:
		b.handleGetBet(ctx, delivery, req, corID)
	case dto.CREATE_BET:
		b.handleCreateBet(ctx, delivery, req, corID)
	default:
		err := fmt.Errorf("unexpected action: %s", req.Action)
		b.discardAndError(ctx, delivery, err)
	}

	return nil
}

func (b *BetService) handleCreateBet(ctx context.Context, delivery amqp.Delivery, req dto.Request, corID string) {
	var createReq dto.CreateBetRequest
	if err := json.Unmarshal(req.Payload, &createReq); err != nil {
		b.discardAndError(ctx, delivery, fmt.Errorf("invalid payload: %w", err))
		return
	}

	isOpen, err := b.isEventOpen(ctx, createReq.EventID)
	if err != nil {
		b.discardAndError(ctx, delivery, fmt.Errorf("validation failed: %w", err))
		return
	}
	if !isOpen {
		b.discardAndError(ctx, delivery, errors.New("event is closed or finished"))
		return
	}

	newID := 999 // TODO: заглушка
	newBet := dto.Bet{
		Id:      newID,
		EventId: createReq.EventID,
		Amount:  createReq.Amount,
		User:    fmt.Sprintf("User_%d", createReq.UserID),
	}

	b.Store.Put(newBet.Id, newBet)

	response, err := dto.PackResponse(newBet, dto.OK, nil)
	if err != nil {
		log.Printf("failed to pack response: %v", err)
		_ = delivery.Nack(false, true)
		return
	}

	if err := b.Broker.PublishWithCorrelationId(ctx, b.ResponseQueue, corID, response); err != nil {
		log.Printf("failed to send reply: %v", err)
		_ = delivery.Nack(false, true)
		return
	}

	_ = delivery.Ack(false)
}

func (b *BetService) handleGetBet(ctx context.Context, delivery amqp.Delivery, req dto.Request, corID string) {
	var getBet dto.GetBetRequest
	if err := json.Unmarshal(req.Payload, &getBet); err != nil {
		b.discardAndError(ctx, delivery, err)
		return
	}

	bet, err := b.Store.Get(getBet.Id)

	var status dto.ReplyStatus
	switch {
	case errors.Is(err, store.ErrNotFound):
		status = dto.NOT_FOUND
	case err != nil:
		status = dto.ERROR
	default:
		status = dto.OK
	}

	response, err := dto.PackResponse(bet, status, err)
	if err != nil {
		log.Printf("failed to pack response: %v", err)
		_ = delivery.Nack(false, true)
		return
	}

	if err := b.Broker.PublishWithCorrelationId(ctx, b.ResponseQueue, corID, response); err != nil {
		log.Printf("failed to send reply: %v", err)
		_ = delivery.Nack(false, true)
		return
	}

	_ = delivery.Ack(false)
}

func (b *BetService) isEventOpen(ctx context.Context, eventID int) (bool, error) {
	status, err := b.EventClient.GetEventStatus(ctx, eventID)
	if err != nil {
		return false, fmt.Errorf("failed to get event status: %w", err)
	}

	return status == dto.EVENT_OPEN, nil
}
