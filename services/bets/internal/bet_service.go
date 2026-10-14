package internal

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"bookmaker/pkg/rmq"
	"bookmaker/pkg/store"
	"bookmaker/shared/dto"

	events "bookmaker/events"

	amqp "github.com/rabbitmq/amqp091-go"
)

// максимальное время ожидания ответа от Events при создании ставки
const eventsCallTimeout = 5 * time.Second

type BetService struct {
	Store       store.Store[dto.Bet]
	Server      *rmq.RPCServer // принимает запросы к Bets
	Client      *rmq.RPCClient // общий клиент для вызовов других сервисов
	EventClient *events.EventClient

	// TODO: пока нет БД, настоящие Id должна выдавать она.
	// atomic: обработчики выполняются конкурентно
	lastBetID atomic.Int64
}

func NewBetService(
	betStore store.Store[dto.Bet],
	conn *amqp.Connection,
	betsRequestsQueue string, // откуда сервис будет читать запросы
	eventsRequestsQueue string, // куда сервис будет посылать запросы для Events
	responsesQueue string, // очередь ответов на запросы Bets к другим сервисам ("" - имя выдаст брокер)
) (*BetService, error) {

	client, err := rmq.NewClient(conn, responsesQueue)
	if err != nil {
		return nil, fmt.Errorf("creating rpc client: %w", err)
	}

	server, err := rmq.NewServer(conn, betsRequestsQueue)
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("creating rpc server: %w", err)
	}

	b := &BetService{
		Store:       betStore,
		Server:      server,
		Client:      client,
		EventClient: events.NewEventClient(client, eventsRequestsQueue),
	}
	b.lastBetID.Store(6) // последняя ставка в заглушке хранилища, следующая будет 7

	return b, nil
}

// устанавливает обработчики и запускает клиент и сервер (каждый в своей горутине)
func (b *BetService) Start(ctx context.Context, errCh chan<- error) error {
	rmq.RegisterHandler(b.Server, dto.GET_BET, b.handleGetBet)
	rmq.RegisterHandler(b.Server, dto.CREATE_BET, b.handleCreateBet)

	// клиент запускаем раньше сервера: обработчики вызывают другие сервисы
	if err := b.Client.Start(ctx, errCh); err != nil {
		return fmt.Errorf("starting rpc client: %w", err)
	}
	if err := b.Server.Start(ctx, errCh); err != nil {
		return fmt.Errorf("starting rpc server: %w", err)
	}

	return nil
}

// Close останавливает сервис. Сначала сервер: он дожидается выполняющихся
// обработчиков, которым ещё может быть нужен клиент. Затем клиент.
// Безопасен, даже если Start не вызывался или завершился ошибкой.
func (b *BetService) Close() error {
	return errors.Join(b.Server.Close(), b.Client.Close())
}

func (b *BetService) handleGetBet(request dto.GetBetRequest) (dto.Bet, rmq.RPCResponseStatus, error) {
	bet, err := b.Store.Get(request.Id)

	status := rmq.STATUS_OK
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			status = dto.STATUS_NOT_FOUND
		} else {
			status = rmq.STATUS_ERROR
		}
	}

	return bet, status, err
}

func (b *BetService) handleCreateBet(request dto.CreateBetRequest) (dto.Bet, rmq.RPCResponseStatus, error) {
	// обработчики не получают контекст, поэтому таймаут задаём здесь
	ctx, cancel := context.WithTimeout(context.Background(), eventsCallTimeout)
	defer cancel()

	isOpen, err := b.isEventOpen(ctx, request.EventID)
	if err != nil {
		return dto.Bet{}, "", fmt.Errorf("validation failed: %w", err)
	}
	if !isOpen {
		return dto.Bet{}, dto.STATUS_EVENT_CLOSED, nil
	}

	newBet := dto.Bet{
		Id:      int(b.lastBetID.Add(1)), // TODO: пока нет БД
		EventId: request.EventID,
		Amount:  request.Amount,
		User:    fmt.Sprintf("User_%d", request.UserID),
	}
	b.Store.Put(newBet.Id, newBet)

	return newBet, rmq.STATUS_OK, nil
}

func (b *BetService) isEventOpen(ctx context.Context, eventID int) (bool, error) {
	status, err := b.EventClient.GetEventStatus(ctx, eventID)
	if err != nil {
		return false, fmt.Errorf("failed to get event status: %w", err)
	}

	return status == dto.EVENT_STATUS_OPEN, nil
}
