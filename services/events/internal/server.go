package internal

import (
	"context"
	"errors"
	"fmt"

	"bookmaker/channels"
	"bookmaker/messages"
	"bookmaker/pkg/genamqp"
	"bookmaker/pkg/store"
	"bookmaker/proto/amqp"
	"bookmaker/schemas"
	"bookmaker/shared/dto"

	"github.com/shopspring/decimal"
)

const (
	msgGetEvent       = "GetEventRequest"
	msgGetEventStatus = "GetEventStatusRequest"
	msgCreateEvent    = "CreateEventRequest"
	msgSetEventStatus = "SetEventStatusRequest"
	msgGetEvents      = "GetEventsRequest"
	msgEventsResponse = "EventsResponse"

	msgEvent               = "Event"
	msgEventStatusResponse = "EventStatusResponse"
	msgErrorResponse       = "ErrorResponse"

	codeNotFound    = "NOT_FOUND"
	codeValidation  = "VALIDATION_ERROR"
	codeEventClosed = "EVENT_CLOSED"
	codeInternal    = "INTERNAL_ERROR"
)

var errBadRequest = errors.New("bad request")

type replyChannel struct {
	out        genamqp.Publishing
	sealEvent  func(amqp.EnvelopeWriter, *messages.EventOut) error
	sealStatus func(amqp.EnvelopeWriter, *messages.EventStatusResponseOut) error
	sealError  func(amqp.EnvelopeWriter, *messages.ErrorResponseOut) error
	sealEvents func(amqp.EnvelopeWriter, *messages.EventsResponseOut) error
}

func betsReply(channel *channels.BetsResponsesAMQP) *replyChannel {
	return &replyChannel{
		out: channel,
		sealEvent: func(e amqp.EnvelopeWriter, m *messages.EventOut) error {
			return channel.SealEvent(e, m)
		},
		sealStatus: func(e amqp.EnvelopeWriter, m *messages.EventStatusResponseOut) error {
			return channel.SealEventStatusResponse(e, m)
		},
		sealError: func(e amqp.EnvelopeWriter, m *messages.ErrorResponseOut) error {
			return channel.SealErrorResponse(e, m)
		},
	}
}

func gatewayReply(channel *channels.GatewayResponsesAMQP) *replyChannel {
	return &replyChannel{
		out: channel,
		sealEvent: func(e amqp.EnvelopeWriter, m *messages.EventOut) error {
			return channel.SealEvent(e, m)
		},
		sealStatus: func(e amqp.EnvelopeWriter, m *messages.EventStatusResponseOut) error {
			return channel.SealEventStatusResponse(e, m)
		},
		sealError: func(e amqp.EnvelopeWriter, m *messages.ErrorResponseOut) error {
			return channel.SealErrorResponse(e, m)
		},
		sealEvents: func(
			envelope amqp.EnvelopeWriter,
			message *messages.EventsResponseOut,
		) error {
			return channel.SealEventsResponse(envelope, message)
		},
	}
}

type response struct {
	event  *schemas.Event
	status *schemas.EventStatusResponse
	events *schemas.EventsResponse
	errMsg *schemas.ErrorResponse
	cause  error
}

func (r *replyChannel) send(ctx context.Context, correlationID string, resp response) error {
	headers := genamqp.Headers{CorrelationID: correlationID}

	switch {
	case resp.event != nil:
		message := (&messages.EventOut{}).SetPayload(*resp.event)
		headers.MessageType = msgEvent
		return genamqp.Publish(ctx, r.out, func(e amqp.EnvelopeWriter) error {
			return r.sealEvent(e, message)
		}, headers)
	case resp.status != nil:
		message := (&messages.EventStatusResponseOut{}).SetPayload(*resp.status)
		headers.MessageType = msgEventStatusResponse
		return genamqp.Publish(ctx, r.out, func(e amqp.EnvelopeWriter) error {
			return r.sealStatus(e, message)
		}, headers)
	case resp.errMsg != nil:
		message := (&messages.ErrorResponseOut{}).SetPayload(*resp.errMsg)
		headers.MessageType = msgErrorResponse
		return genamqp.Publish(ctx, r.out, func(e amqp.EnvelopeWriter) error {
			return r.sealError(e, message)
		}, headers)
	case resp.events != nil:
		message := (&messages.EventsResponseOut{}).SetPayload(*resp.events)
		headers.MessageType = msgEventsResponse

		return genamqp.Publish(ctx, r.out, func(e amqp.EnvelopeWriter) error {
			return r.sealEvents(e, message)
		}, headers)
	default:
		return errors.New("empty response")
	}
}

// Server connects generated message types to event business logic.
type Server struct {
	svc          *EventService
	requests     *channels.EventsRequestsAMQP
	replies      map[string]*replyChannel
	gatewayQueue string
	onError      func(error)
	dispatcher   *genamqp.Dispatcher
}

func NewServer(
	svc *EventService,
	requests *channels.EventsRequestsAMQP,
	bets *channels.BetsResponsesAMQP,
	gateway *channels.GatewayResponsesAMQP,
	onError func(error),
) *Server {
	betsRoute, gatewayRoute := betsReply(bets), gatewayReply(gateway)
	gatewayQueue := gatewayRoute.out.Queue()

	s := &Server{
		svc:          svc,
		requests:     requests,
		replies:      map[string]*replyChannel{betsRoute.out.Queue(): betsRoute, gatewayQueue: gatewayRoute},
		gatewayQueue: gatewayQueue,
		onError:      onError,
		dispatcher:   genamqp.NewDispatcher(onError),
	}

	s.dispatcher.Handle(msgGetEvent, s.serve(true, s.onGetEvent))
	s.dispatcher.Handle(msgGetEventStatus, s.serve(true, s.onGetEventStatus))
	s.dispatcher.Handle(msgCreateEvent, s.serve(false, s.onCreateEvent))
	s.dispatcher.Handle(msgSetEventStatus, s.serve(false, s.onSetEventStatus))
	s.dispatcher.Handle(msgGetEvents, s.serve(false, s.onGetEvents))
	return s
}

func (s *Server) Run(ctx context.Context) error {
	return s.dispatcher.Run(ctx, s.requests.Subscribe)
}

func (s *Server) serve(allowBets bool, handle func(amqp.EnvelopeReader) response) genamqp.Handler {
	return func(ctx context.Context, envelope amqp.EnvelopeReader, meta genamqp.Meta) error {
		if meta.ReplyTo == "" {
			return errors.New("replyTo header is empty")
		}
		if !allowBets && meta.ReplyTo != s.gatewayQueue {
			return fmt.Errorf("replyTo %q is not allowed for this message", meta.ReplyTo)
		}
		route, ok := s.replies[meta.ReplyTo]
		if !ok {
			return fmt.Errorf("unknown replyTo %q", meta.ReplyTo)
		}
		if meta.CorrelationID == "" {
			return errors.New("correlationId header is empty")
		}

		result := handle(envelope)
		if result.errMsg != nil && result.errMsg.Code != nil && *result.errMsg.Code == codeInternal && s.onError != nil {
			s.onError(fmt.Errorf("%s: %w", meta.MessageType, result.cause))
		}
		return route.send(ctx, meta.CorrelationID, result)
	}
}

func (s *Server) onGetEvents(envelope amqp.EnvelopeReader) response {
	var message messages.GetEventsRequestIn

	if err := s.requests.UnsealGetEventsRequest(envelope, &message); err != nil {
		return failure(fmt.Errorf("%w: %w", errBadRequest, err))
	}

	request := message.Payload()

	events, total, err := s.svc.GetEvents(
		request.Status,
		request.Limit,
		request.Offset,
	)
	if err != nil {
		return failure(err)
	}

	result := make([]schemas.Event, 0, len(events))

	for _, event := range events {
		eventID := int64(event.Id)
		coefficient := event.Coefficient.String()
		status := string(statusOf(event))

		result = append(result, schemas.Event{
			EventID:     &eventID,
			Coefficient: &coefficient,
			Status:      &status,
		})
	}

	return response{
		events: &schemas.EventsResponse{
			Events: &result,
			Total:  &total,
		},
	}
}

func (s *Server) onGetEvent(envelope amqp.EnvelopeReader) response {
	var msg messages.GetEventRequestIn
	if err := s.requests.UnsealGetEventRequest(envelope, &msg); err != nil {
		return failure(fmt.Errorf("%w: %w", errBadRequest, err))
	}
	id, err := requireID(msg.Payload().EventID)
	if err != nil {
		return failure(err)
	}
	event, err := s.svc.GetEvent(id)
	if err != nil {
		return failure(err)
	}
	return eventResponse(event)
}

func (s *Server) onGetEventStatus(envelope amqp.EnvelopeReader) response {
	var msg messages.GetEventStatusRequestIn
	if err := s.requests.UnsealGetEventStatusRequest(envelope, &msg); err != nil {
		return failure(fmt.Errorf("%w: %w", errBadRequest, err))
	}
	id, err := requireID(msg.Payload().EventID)
	if err != nil {
		return failure(err)
	}
	status, err := s.svc.GetEventStatus(id)
	if err != nil {
		return failure(err)
	}
	return statusResponse(status)
}

func (s *Server) onCreateEvent(envelope amqp.EnvelopeReader) response {
	var msg messages.CreateEventRequestIn
	if err := s.requests.UnsealCreateEventRequest(envelope, &msg); err != nil {
		return failure(fmt.Errorf("%w: %w", errBadRequest, err))
	}
	raw := msg.Payload().Coefficient
	if raw == nil {
		return failure(fmt.Errorf("%w: coefficient is required", errBadRequest))
	}
	coefficient, err := decimal.NewFromString(*raw)
	if err != nil {
		return failure(fmt.Errorf("%w: coefficient %q: %w", errBadRequest, *raw, err))
	}
	event, err := s.svc.CreateEvent(coefficient)
	if err != nil {
		return failure(err)
	}
	return eventResponse(event)
}

func (s *Server) onSetEventStatus(envelope amqp.EnvelopeReader) response {
	var msg messages.SetEventStatusRequestIn
	if err := s.requests.UnsealSetEventStatusRequest(envelope, &msg); err != nil {
		return failure(fmt.Errorf("%w: %w", errBadRequest, err))
	}
	payload := msg.Payload()
	id, err := requireID(payload.EventID)
	if err != nil {
		return failure(err)
	}
	if payload.Status == nil {
		return failure(fmt.Errorf("%w: status is required", errBadRequest))
	}
	status, err := s.svc.SetEventStatus(id, dto.EventStatus(*payload.Status))
	if err != nil {
		return failure(err)
	}
	return statusResponse(status)
}

func requireID(value *int64) (int, error) {
	if value == nil {
		return 0, fmt.Errorf("%w: event_id is required", errBadRequest)
	}
	return int(*value), nil
}

func eventResponse(event dto.Event) response {
	id, coefficient, status := int64(event.Id), event.Coefficient.String(), string(statusOf(event))
	return response{event: &schemas.Event{EventID: &id, Coefficient: &coefficient, Status: &status}}
}

func statusResponse(value dto.EventStatusResponse) response {
	id, status := int64(value.EventID), string(value.Status)
	return response{status: &schemas.EventStatusResponse{EventID: &id, Status: &status}}
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
	case errors.Is(err, store.ErrNotFound):
		return codeNotFound
	case errors.Is(err, errBadRequest),
		errors.Is(err, ErrInvalidCoefficient),
		errors.Is(err, ErrUnexpectedEventStatus):
		return codeValidation
	case errors.Is(err, ErrEventCannotReopened):
		return codeEventClosed
	default:
		return codeInternal
	}
}
