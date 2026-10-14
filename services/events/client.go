package events

import (
	"context"
	"errors"
	"fmt"

	"bookmaker/pkg/rmq" // путь подставьте свой
	"bookmaker/shared/dto"

	"github.com/shopspring/decimal"
)

// ErrEventNotFound возвращается, если сервис events ответил статусом «не найдено»
var ErrEventNotFound = errors.New("event not found")

// EventClient - типизированная обёртка над общим RPCClient микросервиса
// для вызовов сервиса events.
// Сам RPCClient (очередь ответов, Start/Close) создаёт и закрывает владелец,
// а не EventClient.
type EventClient struct {
	rpc *rmq.RPCClient
}

// NewEventClient регистрирует методы сервиса events в общем RPCClient.
// requestQueue - очередь запросов сервиса events.
func NewEventClient(rpc *rmq.RPCClient, requestQueue string) *EventClient {
	rpc.AddMethod(rmq.RPCMethodName(dto.GET_EVENT), requestQueue)
	rpc.AddMethod(rmq.RPCMethodName(dto.GET_EVENT_STATUS), requestQueue) // TODO: нужно что-то придумать чтобы не забывать тут регистрировать новые методы
	rpc.AddMethod(rmq.RPCMethodName(dto.CREATE_EVENT), requestQueue)
	rpc.AddMethod(rmq.RPCMethodName(dto.SET_EVENT_STATUS), requestQueue)

	return &EventClient{rpc: rpc}
}

func (c *EventClient) GetEventStatus(ctx context.Context, eventID int) (dto.EventStatus, error) {
	reply, status, err := rmq.Call[dto.GetEventRequest, dto.EventStatusReply](
		ctx, c.rpc, rmq.RPCMethodName(dto.GET_EVENT_STATUS), dto.GetEventRequest{Id: eventID},
	)

	// статус проверяем до err: сервер мог вернуть NOT_FOUND и с ошибкой, и без неё
	if status == rmq.RPCResponseStatus(dto.STATUS_NOT_FOUND) {
		return "", fmt.Errorf("event with id %d: %w", eventID, ErrEventNotFound)
	}
	if err != nil {
		return "", fmt.Errorf("get event status from events: %w", err)
	}

	if status != rmq.RPCResponseStatus(rmq.STATUS_OK) {
		return "", fmt.Errorf("unexpected events reply status: %s", status)
	}
	if reply.Status == "" { // ответ без payload
		return "", errors.New("events reply has empty status")
	}

	return reply.Status, nil
}

func (c *EventClient) GetEvent(ctx context.Context, eventID int) (*dto.Event, error) {
	reply, status, err := rmq.Call[dto.GetEventRequest, dto.Event](
		ctx, c.rpc, rmq.RPCMethodName(dto.GET_EVENT), dto.GetEventRequest{Id: eventID},
	)

	// статус проверяем до err: сервер мог вернуть NOT_FOUND и с ошибкой, и без неё
	if status == rmq.RPCResponseStatus(dto.STATUS_NOT_FOUND) {
		return nil, fmt.Errorf("event with id %d: %w", eventID, ErrEventNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("get event from events: %w", err)
	}

	if status != rmq.RPCResponseStatus(rmq.STATUS_OK) {
		return nil, fmt.Errorf("unexpected events reply status: %s", status)
	}

	return reply, nil
}

func (c *EventClient) CreateEvent(ctx context.Context, coefficient decimal.Decimal) (*dto.Event, rmq.RPCResponseStatus, error) {
	return rmq.Call[dto.CreateEventRequest, dto.Event](
		ctx, c.rpc, rmq.RPCMethodName(dto.CREATE_EVENT), dto.CreateEventRequest{Coefficient: coefficient}, // TODO: надо вынести создание запроса наружу
	)
}

func (c *EventClient) SetEventStatus(ctx context.Context, status dto.EventStatus) (*dto.EventStatusReply, rmq.RPCResponseStatus, error) {
	return rmq.Call[dto.SetEventStatusRequest, dto.EventStatusReply](
		ctx, c.rpc, rmq.RPCMethodName(dto.SET_EVENT_STATUS), dto.SetEventStatusRequest{Status: status}, // TODO: надо вынести создание запроса наружу
	)
}
