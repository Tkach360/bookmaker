package events

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"

	"bookmaker/channels"
	"bookmaker/messages"
	"bookmaker/pkg/genamqp"
	"bookmaker/proto/amqp"
	"bookmaker/schemas"
	"bookmaker/shared/dto"

	"github.com/shopspring/decimal"
)

const (
	msgGetEvent            = "GetEventRequest"
	msgGetEventStatus      = "GetEventStatusRequest"
	msgCreateEvent         = "CreateEventRequest"
	msgSetEventStatus      = "SetEventStatusRequest"
	msgEvent               = "Event"
	msgEventStatusResponse = "EventStatusResponse"
	msgErrorResponse       = "ErrorResponse"

	statusOK       = "OK"
	statusNotFound = "NOT_FOUND"
	statusInternal = "INTERNAL_ERROR"
)

// ErrEventNotFound возвращается, если сервис events ответил NOT_FOUND.
var ErrEventNotFound = errors.New("event not found")

type callResult struct {
	event  *schemas.Event
	status *schemas.EventStatusResponse
	code   string
	err    error
}

// EventClient отправляет запросы через сгенерированный AsyncAPI-канал
// events.requests и получает ответы из bets.responses.
type EventClient struct {
	requests   *channels.EventsRequestsAMQP
	responses  *channels.BetsResponsesAMQP
	dispatcher *genamqp.Dispatcher

	mu      sync.Mutex
	pending map[string]chan callResult
}

func NewEventClient(
	requests *channels.EventsRequestsAMQP,
	responses *channels.BetsResponsesAMQP,
	onError func(error),
) *EventClient {
	if onError == nil {
		onError = func(error) {}
	}

	c := &EventClient{
		requests:  requests,
		responses: responses,
		pending:   make(map[string]chan callResult),
	}
	c.dispatcher = genamqp.NewDispatcher(onError)
	c.dispatcher.Handle(msgEvent, c.onEvent)
	c.dispatcher.Handle(msgEventStatusResponse, c.onEventStatusResponse)
	c.dispatcher.Handle(msgErrorResponse, c.onErrorResponse)
	return c
}

// Run читает ответы от events и сопоставляет их с ожидающими запросами.
func (c *EventClient) Run(ctx context.Context) error {
	return c.dispatcher.Run(ctx, c.responses.Subscribe)
}

func (c *EventClient) GetEventStatus(ctx context.Context, eventID int) (dto.EventStatus, error) {
	eventID64 := int64(eventID)
	message := (&messages.GetEventStatusRequestOut{}).SetPayload(schemas.GetEventStatusRequest{
		EventID: &eventID64,
	})

	result, err := c.request(ctx, msgGetEventStatus, func(envelope amqp.EnvelopeWriter) error {
		return c.requests.SealGetEventStatusRequest(envelope, message)
	})
	if err != nil {
		return "", fmt.Errorf("get event status from events: %w", err)
	}
	if result.code == statusNotFound {
		return "", fmt.Errorf("event with id %d: %w", eventID, ErrEventNotFound)
	}
	if result.err != nil {
		return "", fmt.Errorf("get event status from events: %w", result.err)
	}
	if result.code != statusOK {
		return "", fmt.Errorf("unexpected events reply status: %s", result.code)
	}
	if result.status == nil || result.status.Status == nil || *result.status.Status == "" {
		return "", errors.New("events reply has empty status")
	}

	return dto.EventStatus(*result.status.Status), nil
}

func (c *EventClient) GetEvent(ctx context.Context, eventID int) (*dto.Event, error) {
	eventID64 := int64(eventID)
	message := (&messages.GetEventRequestOut{}).SetPayload(schemas.GetEventRequest{
		EventID: &eventID64,
	})

	result, err := c.request(ctx, msgGetEvent, func(envelope amqp.EnvelopeWriter) error {
		return c.requests.SealGetEventRequest(envelope, message)
	})
	if err != nil {
		return nil, fmt.Errorf("get event from events: %w", err)
	}
	if result.code == statusNotFound {
		return nil, fmt.Errorf("event with id %d: %w", eventID, ErrEventNotFound)
	}
	if result.err != nil {
		return nil, fmt.Errorf("get event from events: %w", result.err)
	}
	if result.code != statusOK {
		return nil, fmt.Errorf("unexpected events reply status: %s", result.code)
	}
	if result.event == nil {
		return nil, errors.New("events reply has empty event")
	}

	event, err := decodeEvent(*result.event)
	if err != nil {
		return nil, fmt.Errorf("decode event response: %w", err)
	}
	return event, nil
}

func (c *EventClient) CreateEvent(
	ctx context.Context,
	coefficient decimal.Decimal,
) (*dto.Event, string, error) {
	coefficientString := coefficient.String()
	message := (&messages.CreateEventRequestOut{}).SetPayload(schemas.CreateEventRequest{
		Coefficient: &coefficientString,
	})

	result, err := c.request(ctx, msgCreateEvent, func(envelope amqp.EnvelopeWriter) error {
		return c.requests.SealCreateEventRequest(envelope, message)
	})
	if err != nil {
		return nil, "", err
	}
	if result.err != nil {
		return nil, result.code, result.err
	}
	if result.code != statusOK {
		return nil, result.code, fmt.Errorf("unexpected events reply status: %s", result.code)
	}
	if result.event == nil {
		return nil, result.code, errors.New("events reply has empty event")
	}
	event, err := decodeEvent(*result.event)
	if err != nil {
		return nil, result.code, fmt.Errorf("decode event response: %w", err)
	}
	return event, result.code, nil
}

func (c *EventClient) SetEventStatus(
	ctx context.Context,
	eventID int,
	status dto.EventStatus,
) (*dto.EventStatusResponse, string, error) {
	eventID64 := int64(eventID)
	statusString := string(status)
	message := (&messages.SetEventStatusRequestOut{}).SetPayload(schemas.SetEventStatusRequest{
		EventID: &eventID64,
		Status:  &statusString,
	})

	result, err := c.request(ctx, msgSetEventStatus, func(envelope amqp.EnvelopeWriter) error {
		return c.requests.SealSetEventStatusRequest(envelope, message)
	})
	if err != nil {
		return nil, "", err
	}
	if result.err != nil {
		return nil, result.code, result.err
	}
	if result.code != statusOK {
		return nil, result.code, fmt.Errorf("unexpected events reply status: %s", result.code)
	}
	if result.status == nil || result.status.EventID == nil || result.status.Status == nil {
		return nil, result.code, errors.New("events reply has incomplete event status")
	}

	return &dto.EventStatusResponse{
		EventID: int(*result.status.EventID),
		Status:  dto.EventStatus(*result.status.Status),
	}, result.code, nil
}

func (c *EventClient) request(
	ctx context.Context,
	messageType string,
	seal func(amqp.EnvelopeWriter) error,
) (callResult, error) {
	return c.call(ctx, func(ctx context.Context, correlationID string) error {
		return genamqp.Publish(ctx, c.requests, seal, genamqp.Headers{
			MessageType:   messageType,
			CorrelationID: correlationID,
			ReplyTo:       c.responses.Queue(),
		})
	})
}

func (c *EventClient) call(
	ctx context.Context,
	publish func(context.Context, string) error,
) (callResult, error) {
	correlationID, err := newCorrelationID()
	if err != nil {
		return callResult{}, err
	}

	resultCh := make(chan callResult, 1)
	c.mu.Lock()
	c.pending[correlationID] = resultCh
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pending, correlationID)
		c.mu.Unlock()
	}()

	if err := publish(ctx, correlationID); err != nil {
		return callResult{}, err
	}

	select {
	case result := <-resultCh:
		return result, nil
	case <-ctx.Done():
		return callResult{}, ctx.Err()
	}
}

func (c *EventClient) onEvent(
	_ context.Context,
	envelope amqp.EnvelopeReader,
	meta genamqp.Meta,
) error {
	if meta.CorrelationID == "" {
		return errors.New("event response has empty correlationId")
	}
	var message messages.EventIn
	if err := c.responses.UnsealEvent(envelope, &message); err != nil {
		return err
	}
	payload := message.Payload()
	c.deliver(meta.CorrelationID, callResult{event: &payload, code: statusOK})
	return nil
}

func (c *EventClient) onEventStatusResponse(
	_ context.Context,
	envelope amqp.EnvelopeReader,
	meta genamqp.Meta,
) error {
	if meta.CorrelationID == "" {
		return errors.New("event status response has empty correlationId")
	}
	var message messages.EventStatusResponseIn
	if err := c.responses.UnsealEventStatusResponse(envelope, &message); err != nil {
		return err
	}
	payload := message.Payload()
	c.deliver(meta.CorrelationID, callResult{status: &payload, code: statusOK})
	return nil
}

func (c *EventClient) onErrorResponse(
	_ context.Context,
	envelope amqp.EnvelopeReader,
	meta genamqp.Meta,
) error {
	if meta.CorrelationID == "" {
		return errors.New("event error response has empty correlationId")
	}
	var message messages.ErrorResponseIn
	if err := c.responses.UnsealErrorResponse(envelope, &message); err != nil {
		return err
	}
	payload := message.Payload()
	code, text := stringValue(payload.Code), stringValue(payload.Message)
	if code == "" {
		code = statusInternal
	}
	if text == "" {
		text = code
	}
	c.deliver(meta.CorrelationID, callResult{code: code, err: errors.New(text)})
	return nil
}

func (c *EventClient) deliver(correlationID string, result callResult) {
	c.mu.Lock()
	resultCh := c.pending[correlationID]
	c.mu.Unlock()
	if resultCh == nil {
		return
	}
	select {
	case resultCh <- result:
	default:
	}
}

func decodeEvent(value schemas.Event) (*dto.Event, error) {
	if value.EventID == nil || value.Coefficient == nil || value.Status == nil {
		return nil, errors.New("event response is missing required fields")
	}
	coefficient, err := decimal.NewFromString(*value.Coefficient)
	if err != nil {
		return nil, fmt.Errorf("invalid coefficient: %w", err)
	}
	status := dto.EventStatus(*value.Status)
	return &dto.Event{
		Id:          int(*value.EventID),
		Coefficient: coefficient,
		IsWin:       status == dto.EVENT_STATUS_WIN,
		IsTook:      status != dto.EVENT_STATUS_OPEN,
	}, nil
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
