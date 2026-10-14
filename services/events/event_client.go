package events

import (
	"context"
	"errors"
	"fmt"

	"bookmaker/pkg/rmq" // путь подставьте свой
	"bookmaker/shared/dto"
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

	return &EventClient{rpc: rpc}
}

func (c *EventClient) GetEventStatus(ctx context.Context, eventID int) (dto.EventStatus, error) {
	reply, status, err := rmq.Call[map[string]int, dto.EventStatusReply](
		ctx, c.rpc, rmq.RPCMethodName(dto.GET_EVENT_STATUS), map[string]int{"event_id": eventID},
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
