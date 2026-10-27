package bets

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"

	"bookmaker/channels"
	"bookmaker/messages"
	"bookmaker/pkg/genamqp"
	"bookmaker/proto/amqp"
	"bookmaker/schemas"

	"github.com/shopspring/decimal"
)

const (
	msgGetBet         = "GetBetRequest"
	msgCreateBet      = "CreateBetRequest"
	msgBet            = "Bet"
	msgErrorResponse  = "ErrorResponse"
	statusOK          = "OK"
	msgGetBetDetails  = "GetBetDetailsRequest"
	msgBetDetails     = "BetDetails"
	msgGetBetsByEvent = "GetBetsByEventRequest"
	msgBetsByEvent    = "BetsByEvent"
	msgGetEvents      = "GetEventsRequest"
	msgEventsResponse = "EventsResponse"
)

type callResult struct {
	bet         *schemas.Bet
	status      string
	err         error
	betDetails  *schemas.BetDetails
	betsByEvent *schemas.BetsByEvent
	events      *schemas.EventsResponse
}

// BetClient использует сгенерированные сообщения и каналы go-asyncapi.
type BetClient struct {
	requests       *channels.BetsRequestsAMQP
	responses      *channels.GatewayResponsesAMQP
	dispatcher     *genamqp.Dispatcher
	eventsRequests *channels.EventsRequestsAMQP

	mu      sync.Mutex
	pending map[string]chan callResult
}

func NewBetClient(
	requests *channels.BetsRequestsAMQP,
	eventsRequests *channels.EventsRequestsAMQP,
	responses *channels.GatewayResponsesAMQP,
	onError func(error),
) *BetClient {
	if onError == nil {
		onError = func(error) {}
	}

	c := &BetClient{
		requests:       requests,
		responses:      responses,
		pending:        make(map[string]chan callResult),
		eventsRequests: eventsRequests,
	}
	c.dispatcher = genamqp.NewDispatcher(onError)
	c.dispatcher.Handle(msgBet, c.onBet)
	c.dispatcher.Handle(msgErrorResponse, c.onErrorResponse)
	c.dispatcher.Handle(msgBetDetails, c.onBetDetails)
	c.dispatcher.Handle(msgBetsByEvent, c.onBetsByEvent)
	c.dispatcher.Handle(msgEventsResponse, c.onEventsResponse)
	return c
}

// Run читает ответы из gateway.responses и сопоставляет их с запросами.
func (b *BetClient) Run(ctx context.Context) error {
	return b.dispatcher.Run(ctx, b.responses.Subscribe)
}

func (b *BetClient) GetEvents(
	ctx context.Context,
	status string,
	limit, offset int,
) (*schemas.EventsResponse, string, error) {

	request := schemas.GetEventsRequest{
		Limit:  limit,
		Offset: offset,
	}

	if status != "" {
		request.Status = status
	}

	message := (&messages.GetEventsRequestOut{}).SetPayload(request)

	return call(b, ctx, func(ctx context.Context, correlationID string) error {
		return genamqp.Publish(ctx, b.eventsRequests, func(envelope amqp.EnvelopeWriter) error {
			return b.eventsRequests.SealGetEventsRequest(envelope, message)
		}, genamqp.Headers{
			MessageType:   msgGetEvents,
			CorrelationID: correlationID,
			ReplyTo:       b.responses.Queue(),
		})
	}, func(result callResult) (*schemas.EventsResponse, string, error) {
		return result.events, result.status, result.err
	})
}

func (b *BetClient) onEventsResponse(
	_ context.Context,
	envelope amqp.EnvelopeReader,
	meta genamqp.Meta,
) error {
	if meta.CorrelationID == "" {
		return errors.New("events response has empty correlationId")
	}

	var message messages.EventsResponseIn
	if err := b.responses.UnsealEventsResponse(envelope, &message); err != nil {
		return err
	}

	payload := message.Payload()

	b.deliver(meta.CorrelationID, callResult{
		events: &payload,
		status: statusOK,
	})

	return nil
}

func (b *BetClient) GetBet(ctx context.Context, id int) (*schemas.Bet, string, error) {
	id64 := int64(id)
	message := (&messages.GetBetRequestOut{}).SetPayload(schemas.GetBetRequest{
		ID: &id64,
	})

	return call(b, ctx, func(ctx context.Context, correlationID string) error {
		return genamqp.Publish(ctx, b.requests, func(envelope amqp.EnvelopeWriter) error {
			return b.requests.SealGetBetRequest(envelope, message)
		}, genamqp.Headers{
			MessageType:   msgGetBet,
			CorrelationID: correlationID,
			ReplyTo:       b.responses.Queue(),
		})
	}, func(result callResult) (*schemas.Bet, string, error) {
		return result.bet, result.status, result.err
	})
}

func (b *BetClient) GetBetsByEvent(
	ctx context.Context,
	eventID int,
) (*schemas.BetsByEvent, string, error) {
	eventID64 := int64(eventID)

	message := (&messages.GetBetsByEventRequestOut{}).SetPayload(
		schemas.GetBetsByEventRequest{
			EventID: &eventID64,
		},
	)

	return call(b, ctx, func(ctx context.Context, correlationID string) error {
		return genamqp.Publish(ctx, b.requests, func(envelope amqp.EnvelopeWriter) error {
			return b.requests.SealGetBetsByEventRequest(envelope, message)
		}, genamqp.Headers{
			MessageType:   msgGetBetsByEvent,
			CorrelationID: correlationID,
			ReplyTo:       b.responses.Queue(),
		})
	}, func(result callResult) (*schemas.BetsByEvent, string, error) {
		return result.betsByEvent, result.status, result.err
	})
}

func (b *BetClient) CreateBet(
	ctx context.Context,
	userID, eventID int,
	amount decimal.Decimal,
) (*schemas.Bet, string, error) {
	userID64, eventID64 := int64(userID), int64(eventID)
	amountString := amount.String()
	message := (&messages.CreateBetRequestOut{}).SetPayload(schemas.CreateBetRequest{
		UserID:  &userID64,
		EventID: &eventID64,
		Amount:  &amountString,
	})

	return call(b, ctx, func(ctx context.Context, correlationID string) error {
		return genamqp.Publish(ctx, b.requests, func(envelope amqp.EnvelopeWriter) error {
			return b.requests.SealCreateBetRequest(envelope, message)
		}, genamqp.Headers{
			MessageType:   msgCreateBet,
			CorrelationID: correlationID,
			ReplyTo:       b.responses.Queue(),
		})
	}, func(result callResult) (*schemas.Bet, string, error) {
		return result.bet, result.status, result.err
	})
}

func (b *BetClient) GetBetDetails(ctx context.Context, id int) (*schemas.BetDetails, string, error) {
	id64 := int64(id)
	message := (&messages.GetBetDetailsRequestOut{}).SetPayload(schemas.GetBetDetailsRequest{
		ID: &id64,
	})

	return call(b, ctx, func(ctx context.Context, correlationID string) error {
		return genamqp.Publish(ctx, b.requests, func(envelope amqp.EnvelopeWriter) error {
			return b.requests.SealGetBetDetailsRequest(envelope, message)
		}, genamqp.Headers{
			MessageType:   msgGetBetDetails,
			CorrelationID: correlationID,
			ReplyTo:       b.responses.Queue(),
		})
	}, func(result callResult) (*schemas.BetDetails, string, error) {
		return result.betDetails, result.status, result.err
	})
}

func (b *BetClient) onBetDetails(
	_ context.Context,
	envelope amqp.EnvelopeReader,
	meta genamqp.Meta,
) error {
	if meta.CorrelationID == "" {
		return errors.New("bet details response has empty correlationId")
	}

	var message messages.BetDetailsIn
	if err := b.responses.UnsealBetDetails(envelope, &message); err != nil {
		return err
	}
	payload := message.Payload()
	b.deliver(meta.CorrelationID, callResult{
		betDetails: &payload,
		status:     statusOK,
	})
	return nil
}

func call[T any](
	b *BetClient,
	ctx context.Context,
	publish func(context.Context, string) error,
	extract func(callResult) (*T, string, error),
) (*T, string, error) {
	correlationID, err := newCorrelationID()
	if err != nil {
		return nil, "", err
	}

	resultCh := make(chan callResult, 1)
	b.mu.Lock()
	b.pending[correlationID] = resultCh
	b.mu.Unlock()

	defer func() {
		b.mu.Lock()
		delete(b.pending, correlationID)
		b.mu.Unlock()
	}()

	if err := publish(ctx, correlationID); err != nil {
		return nil, "", err
	}

	select {
	case result := <-resultCh:
		return extract(result)
	case <-ctx.Done():
		return nil, "", ctx.Err()
	}
}

func (b *BetClient) onBet(
	_ context.Context,
	envelope amqp.EnvelopeReader,
	meta genamqp.Meta,
) error {
	if meta.CorrelationID == "" {
		return errors.New("bet response has empty correlationId")
	}

	var message messages.BetIn
	if err := b.responses.UnsealBet(envelope, &message); err != nil {
		return err
	}
	payload := message.Payload()
	b.deliver(meta.CorrelationID, callResult{
		bet:    &payload,
		status: statusOK,
	})
	return nil
}

func (b *BetClient) onErrorResponse(
	_ context.Context,
	envelope amqp.EnvelopeReader,
	meta genamqp.Meta,
) error {
	if meta.CorrelationID == "" {
		return errors.New("error response has empty correlationId")
	}

	var message messages.ErrorResponseIn
	if err := b.responses.UnsealErrorResponse(envelope, &message); err != nil {
		return err
	}

	payload := message.Payload()
	status := stringValue(payload.Code)
	text := stringValue(payload.Message)
	if status == "" {
		status = "INTERNAL_ERROR"
	}
	if text == "" {
		text = status
	}

	b.deliver(meta.CorrelationID, callResult{
		status: status,
		err:    errors.New(text),
	})
	return nil
}

func (b *BetClient) onBetsByEvent(
	_ context.Context,
	envelope amqp.EnvelopeReader,
	meta genamqp.Meta,
) error {
	if meta.CorrelationID == "" {
		return errors.New("bets by event response has empty correlationId")
	}

	var message messages.BetsByEventIn
	if err := b.responses.UnsealBetsByEvent(envelope, &message); err != nil {
		return err
	}

	payload := message.Payload()

	b.deliver(meta.CorrelationID, callResult{
		betsByEvent: &payload,
		status:      statusOK,
	})

	return nil
}

func (b *BetClient) deliver(correlationID string, result callResult) {
	b.mu.Lock()
	resultCh := b.pending[correlationID]
	b.mu.Unlock()
	if resultCh == nil {
		return // Запрос уже завершился по таймауту или отмене контекста.
	}

	select {
	case resultCh <- result:
	default:
	}
}

func newCorrelationID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(id[:]), nil
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
