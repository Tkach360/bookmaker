package internal

import (
	"context"
	"errors"
	"fmt"

	"bookmaker/pkg/rmq"
	"bookmaker/pkg/store"
	"bookmaker/shared/dto"
)

type BetService struct {
	Store       store.Store[dto.Bet]
	Server      *rmq.RPCServer
	EventClient *EventClient
}

// устанавливает обработчики и запускает сервис в отдельной горутине
func (b *BetService) Start(ctx context.Context, errCh chan<- error) error {
	rmq.RegisterHandler(b.Server, dto.GET_BET, b.handleGetBet)
	rmq.RegisterHandler(b.Server, dto.CREATE_BET, b.handleCreateBet)
	return b.Server.Start(ctx, errCh)
}

func (b *BetService) handleGetBet(request dto.GetBetRequest) (dto.Bet, rmq.RPCResponseStatus, error) {
	bet, err := b.Store.Get(request.Id)

	status := dto.STATUS_OK
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			status = dto.STATUS_NOT_FOUND
		} else {
			status = dto.STATUS_ERROR
		}
	}

	return bet, status, err
}

func (b *BetService) handleCreateBet(request dto.CreateBetRequest) (dto.Bet, rmq.RPCResponseStatus, error) {
	ctx := context.TODO()
	isOpen, err := b.isEventOpen(ctx, request.EventID)
	if err != nil {
		return dto.Bet{}, "", fmt.Errorf("validation failed: %w", err)
	}
	if !isOpen {
		return dto.Bet{}, dto.STATUS_EVENT_CLOSED, nil
	}

	newID := 999 // TODO: заглушка
	newBet := dto.Bet{
		Id:      newID,
		EventId: request.EventID,
		Amount:  request.Amount,
		User:    fmt.Sprintf("User_%d", request.UserID),
	}
	b.Store.Put(newBet.Id, newBet)
	return newBet, dto.STATUS_OK, nil
}

func (b *BetService) isEventOpen(ctx context.Context, eventID int) (bool, error) {
	status, err := b.EventClient.GetEventStatus(ctx, eventID)
	if err != nil {
		return false, fmt.Errorf("failed to get event status: %w", err)
	}

	return status == dto.EVENT_OPEN, nil
}
