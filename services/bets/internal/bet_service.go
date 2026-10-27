package internal

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"bookmaker/channels"
	"bookmaker/events"
	"bookmaker/messages"
	"bookmaker/pkg/genamqp"
	"bookmaker/pkg/store"
	"bookmaker/proto/amqp"
	"bookmaker/schemas"
	"bookmaker/shared/dto"

	"github.com/shopspring/decimal"
)

const (
	eventsCallTimeout = 5 * time.Second

	msgGetBet        = "GetBetRequest"
	msgCreateBet     = "CreateBetRequest"
	msgBet           = "Bet"
	msgErrorResponse = "ErrorResponse"

	codeNotFound    = "NOT_FOUND"
	codeValidation  = "VALIDATION_ERROR"
	codeEventClosed = "EVENT_CLOSED"
	codeInternal    = "INTERNAL_ERROR"

	msgGetBetDetails = "GetBetDetailsRequest"
	msgBetDetails    = "BetDetails"

	msgGetBetsByEvent = "GetBetsByEventRequest"
	msgBetsByEvent    = "BetsByEvent"
)

var (
	errBadRequest  = errors.New("bad request")
	errEventClosed = errors.New("event is closed or does not accept bets")
)

type response struct {
	bet         *schemas.Bet
	errMsg      *schemas.ErrorResponse
	cause       error
	betDetails  *schemas.BetDetails
	betsByEvent *schemas.BetsByEvent
}

// BetService содержит бизнес-логику ставок и связывает её со сгенерированными
// AsyncAPI-каналами. Старый RPCClient/RPCServer здесь больше не используются.
type BetService struct {
	Store store.Store[dto.Bet]

	requests         *channels.BetsRequestsAMQP
	gatewayResponses *channels.GatewayResponsesAMQP
	EventClient      *events.EventClient

	dispatcher *genamqp.Dispatcher
	onError    func(error)

	lastBetID atomic.Int64

	startMu      sync.Mutex
	cancelServer context.CancelFunc
	cancelClient context.CancelFunc
	started      bool
	wg           sync.WaitGroup
}

func NewBetService(
	betStore store.Store[dto.Bet],
	requests *channels.BetsRequestsAMQP,
	eventsRequests *channels.EventsRequestsAMQP,
	betsResponses *channels.BetsResponsesAMQP,
	gatewayResponses *channels.GatewayResponsesAMQP,
	onError func(error),
) *BetService {
	if onError == nil {
		onError = func(error) {}
	}

	b := &BetService{
		Store:            betStore,
		requests:         requests,
		gatewayResponses: gatewayResponses,
		onError:          onError,
		dispatcher:       genamqp.NewDispatcher(onError),
	}
	b.EventClient = events.NewEventClient(eventsRequests, betsResponses, onError)
	b.lastBetID.Store(6) // последняя ставка в заглушечном хранилище

	b.dispatcher.Handle(msgGetBet, b.serve(b.onGetBet))
	b.dispatcher.Handle(msgCreateBet, b.serve(b.onCreateBet))
	b.dispatcher.Handle(msgGetBetDetails, b.serve(b.onGetBetDetails))
	b.dispatcher.Handle(msgGetBetsByEvent, b.serve(b.onGetBetsByEvent))
	return b
}

// Start запускает обработчик запросов Bets и клиент, принимающий ответы Events.
func (b *BetService) Start(ctx context.Context, errCh chan<- error) error {
	b.startMu.Lock()
	if b.started {
		b.startMu.Unlock()
		return errors.New("bet service already started")
	}
	serverCtx, cancelServer := context.WithCancel(ctx)
	clientCtx, cancelClient := context.WithCancel(context.Background())
	b.cancelServer = cancelServer
	b.cancelClient = cancelClient
	b.started = true
	b.wg.Add(2)
	b.startMu.Unlock()

	// Клиент Events живёт дольше приёмника запросов: уже начатая обработка
	// CreateBet может получить ответ Events во время graceful shutdown.
	go func() {
		defer b.wg.Done()
		err := b.EventClient.Run(clientCtx)
		if err != nil && !errors.Is(err, context.Canceled) {
			reportError(errCh, fmt.Errorf("events response client: %w", err))
		} else if clientCtx.Err() == nil {
			reportError(errCh, errors.New("events response client stopped unexpectedly"))
		}
	}()

	go func() {
		defer b.wg.Done()
		err := b.dispatcher.Run(serverCtx, b.requests.Subscribe)
		if err != nil && !errors.Is(err, context.Canceled) {
			reportError(errCh, fmt.Errorf("bets request server: %w", err))
		} else if serverCtx.Err() == nil {
			reportError(errCh, errors.New("bets request server stopped unexpectedly: consumer closed"))
		}
		cancelClient()
	}()

	return nil
}

// Close останавливает сервер, затем клиент Events, и дожидается фоновых циклов.
func (b *BetService) Close() error {
	b.startMu.Lock()
	cancelServer, cancelClient, started := b.cancelServer, b.cancelClient, b.started
	b.startMu.Unlock()
	if cancelServer != nil {
		cancelServer()
	}
	if !started {
		return nil
	}
	b.wg.Wait()
	if cancelClient != nil {
		cancelClient()
	}
	return nil
}

func (b *BetService) serve(handle func(context.Context, amqp.EnvelopeReader) response) genamqp.Handler {
	return func(ctx context.Context, envelope amqp.EnvelopeReader, meta genamqp.Meta) error {
		if meta.ReplyTo == "" {
			return errors.New("replyTo header is empty")
		}
		if meta.ReplyTo != b.gatewayResponses.Queue() {
			return fmt.Errorf("replyTo %q is not allowed for Bets", meta.ReplyTo)
		}
		if meta.CorrelationID == "" {
			return errors.New("correlationId header is empty")
		}

		result := handle(ctx, envelope)
		if result.errMsg != nil && result.errMsg.Code != nil && *result.errMsg.Code == codeInternal && b.onError != nil {
			b.onError(fmt.Errorf("%s: %w", meta.MessageType, result.cause))
		}
		return b.send(ctx, meta.CorrelationID, result)
	}
}

func (b *BetService) send(ctx context.Context, correlationID string, result response) error {
	headers := genamqp.Headers{CorrelationID: correlationID}

	switch {
	case result.bet != nil:
		message := (&messages.BetOut{}).SetPayload(*result.bet)
		headers.MessageType = msgBet
		return genamqp.Publish(ctx, b.gatewayResponses, func(envelope amqp.EnvelopeWriter) error {
			return b.gatewayResponses.SealBet(envelope, message)
		}, headers)
	case result.errMsg != nil:
		message := (&messages.ErrorResponseOut{}).SetPayload(*result.errMsg)
		headers.MessageType = msgErrorResponse
		return genamqp.Publish(ctx, b.gatewayResponses, func(envelope amqp.EnvelopeWriter) error {
			return b.gatewayResponses.SealErrorResponse(envelope, message)
		}, headers)
	case result.betDetails != nil:
		message := (&messages.BetDetailsOut{}).SetPayload(*result.betDetails)
		headers.MessageType = msgBetDetails
		return genamqp.Publish(ctx, b.gatewayResponses, func(envelope amqp.EnvelopeWriter) error {
			return b.gatewayResponses.SealBetDetails(envelope, message)
		}, headers)
	case result.betsByEvent != nil:
		message := (&messages.BetsByEventOut{}).SetPayload(*result.betsByEvent)
		headers.MessageType = msgBetsByEvent

		return genamqp.Publish(ctx, b.gatewayResponses, func(envelope amqp.EnvelopeWriter) error {
			return b.gatewayResponses.SealBetsByEvent(envelope, message)
		}, headers)
	default:
		return errors.New("empty response")
	}
}

func (b *BetService) onGetBetDetails(ctx context.Context, envelope amqp.EnvelopeReader) response {
	var message messages.GetBetDetailsRequestIn
	if err := b.requests.UnsealGetBetDetailsRequest(envelope, &message); err != nil {
		return failure(fmt.Errorf("%w: %w", errBadRequest, err))
	}
	id := message.Payload().ID
	if id == nil {
		return failure(fmt.Errorf("%w: id is required", errBadRequest))
	}

	bet, err := b.Store.Get(int(*id))
	if err != nil {
		return failure(err)
	}

	eventCtx, cancel := context.WithTimeout(ctx, eventsCallTimeout)
	defer cancel()
	event, err := b.EventClient.GetEvent(eventCtx, bet.EventId)
	if err != nil {
		return failure(fmt.Errorf("get event for bet %d: %w", bet.Id, err))
	}

	return betDetailsResponse(bet, *event)
}

func (b *BetService) onGetBet(_ context.Context, envelope amqp.EnvelopeReader) response {
	var message messages.GetBetRequestIn
	if err := b.requests.UnsealGetBetRequest(envelope, &message); err != nil {
		return failure(fmt.Errorf("%w: %w", errBadRequest, err))
	}
	id := message.Payload().ID
	if id == nil {
		return failure(fmt.Errorf("%w: id is required", errBadRequest))
	}

	bet, err := b.Store.Get(int(*id))
	if err != nil {
		return failure(err)
	}
	return betResponse(bet)
}

func (b *BetService) onGetBetsByEvent(
	_ context.Context,
	envelope amqp.EnvelopeReader,
) response {
	var message messages.GetBetsByEventRequestIn

	if err := b.requests.UnsealGetBetsByEventRequest(envelope, &message); err != nil {
		return failure(fmt.Errorf("%w: %w", errBadRequest, err))
	}

	eventID := message.Payload().EventID
	if eventID == nil || *eventID <= 0 {
		return failure(fmt.Errorf("%w: event_id must be positive", errBadRequest))
	}

	bets, err := b.Store.FindBy(func(bet dto.Bet) bool {
		return bet.EventId == int(*eventID)
	})
	if err != nil {
		return failure(fmt.Errorf("find bets for event %d: %w", *eventID, err))
	}

	resultBets := make([]schemas.Bet, 0, len(bets))
	for _, bet := range bets {
		result := betResponse(bet)
		if result.bet != nil {
			resultBets = append(resultBets, *result.bet)
		}
	}

	eventIDValue := *eventID

	return response{
		betsByEvent: &schemas.BetsByEvent{
			EventID: &eventIDValue,
			Bets:    &resultBets,
		},
	}
}

func (b *BetService) onCreateBet(_ context.Context, envelope amqp.EnvelopeReader) response {
	var message messages.CreateBetRequestIn
	if err := b.requests.UnsealCreateBetRequest(envelope, &message); err != nil {
		return failure(fmt.Errorf("%w: %w", errBadRequest, err))
	}
	payload := message.Payload()
	if payload.UserID == nil || payload.EventID == nil || payload.Amount == nil {
		return failure(fmt.Errorf("%w: user_id, event_id and amount are required", errBadRequest))
	}
	amount, err := decimal.NewFromString(*payload.Amount)
	if err != nil {
		return failure(fmt.Errorf("%w: invalid amount: %w", errBadRequest, err))
	}

	bet, err := b.createBet(int(*payload.UserID), int(*payload.EventID), amount)
	if err != nil {
		return failure(err)
	}
	return betResponse(bet)
}

func (b *BetService) createBet(userID, eventID int, amount decimal.Decimal) (dto.Bet, error) {
	// Обработчик запроса может выполняться дольше одного сообщения; на вызов Events
	// ограничиваем ожидание пятью секундами, как и в прежнем RPC-клиенте.
	eventCtx, cancel := context.WithTimeout(context.Background(), eventsCallTimeout)
	defer cancel()

	isOpen, err := b.isEventOpen(eventCtx, eventID)
	if err != nil {
		return dto.Bet{}, fmt.Errorf("validation failed: %w", err)
	}
	if !isOpen {
		return dto.Bet{}, errEventClosed
	}

	bet := dto.Bet{
		Id:      int(b.lastBetID.Add(1)),
		EventId: eventID,
		Amount:  amount,
		User:    fmt.Sprintf("User_%d", userID),
	}
	// Сохраняем прежнее поведение: ошибки временного MapStore не меняют ответ клиенту.
	_ = b.Store.Put(bet.Id, bet)
	return bet, nil
}

func (b *BetService) isEventOpen(ctx context.Context, eventID int) (bool, error) {
	status, err := b.EventClient.GetEventStatus(ctx, eventID)
	if err != nil {
		return false, fmt.Errorf("failed to get event status: %w", err)
	}
	return status == dto.EVENT_STATUS_OPEN, nil
}

func betResponse(bet dto.Bet) response {
	id, eventID := int64(bet.Id), int64(bet.EventId)
	amount, user := bet.Amount.String(), bet.User
	return response{bet: &schemas.Bet{
		ID:      &id,
		EventID: &eventID,
		Amount:  &amount,
		User:    &user,
	}}
}

func betDetailsResponse(bet dto.Bet, event dto.Event) response {
	eventID := int64(event.Id)
	coefficient := event.Coefficient.String()
	status := string(eventStatus(event))

	return response{betDetails: &schemas.BetDetails{
		Bet: betSchema(bet),
		Event: &schemas.Event{
			EventID:     &eventID,
			Coefficient: &coefficient,
			Status:      &status,
		},
	}}
}

func betSchema(bet dto.Bet) *schemas.Bet {
	id, eventID := int64(bet.Id), int64(bet.EventId)
	amount, user := bet.Amount.String(), bet.User
	return &schemas.Bet{
		ID:      &id,
		EventID: &eventID,
		Amount:  &amount,
		User:    &user,
	}
}

func eventStatus(event dto.Event) dto.EventStatus {
	if !event.IsTook {
		return dto.EVENT_STATUS_OPEN
	}
	if event.IsWin {
		return dto.EVENT_STATUS_WIN
	}
	return dto.EVENT_STATUS_LOSS
}

func failure(err error) response {
	code, message := errorCode(err), err.Error()
	if code == codeInternal {
		message = "internal error"
	}
	return response{
		errMsg: &schemas.ErrorResponse{Code: &code, Message: &message},
		cause:  err,
	}
}

func errorCode(err error) string {
	switch {
	case errors.Is(err, store.ErrNotFound), errors.Is(err, events.ErrEventNotFound):
		return codeNotFound
	case errors.Is(err, errBadRequest):
		return codeValidation
	case errors.Is(err, errEventClosed):
		return codeEventClosed
	default:
		return codeInternal
	}
}

func reportError(errCh chan<- error, err error) {
	if errCh == nil || err == nil {
		return
	}
	select {
	case errCh <- err:
	default:
	}
}
