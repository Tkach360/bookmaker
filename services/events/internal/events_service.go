package internal

import (
	"context"
	"errors"
	"log"
	"sync/atomic"

	"bookmaker/pkg/rmq"
	"bookmaker/pkg/store"
	"bookmaker/shared/dto"

	"github.com/shopspring/decimal"
)

type EventService struct {
	Store  store.Store[dto.Event]
	Server *rmq.RPCServer

	lastEventId atomic.Int64 // TODO: пока нет БД
}

// устанавливает обработчики и запускает сервис в отдельной горутине
func (e *EventService) Start(ctx context.Context, errCh chan<- error) error {

	e.lastEventId.Store(9) // TODO: пока нет БД

	rmq.RegisterHandler(e.Server, dto.GET_EVENT, e.handleGetEvent)
	rmq.RegisterHandler(e.Server, dto.GET_EVENT_STATUS, e.handleGetEventStatus)
	rmq.RegisterHandler(e.Server, dto.CREATE_EVENT, e.handleCreateEvent)
	return e.Server.Start(ctx, errCh)
}

func (e *EventService) handleGetEvent(request dto.GetEventRequest) (dto.Event, rmq.RPCResponseStatus, error) {
	event, err := e.Store.Get(request.Id)

	status := rmq.STATUS_OK
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			status = dto.STATUS_NOT_FOUND
		} else {
			status = rmq.STATUS_ERROR
		}
	}

	log.Printf("GetEventStatus: eventId: %d; status: %s;", event.Id, status)
	return event, status, err
}

func (e *EventService) handleGetEventStatus(request dto.GetEventRequest) (dto.EventStatusReply, rmq.RPCResponseStatus, error) {
	event, err := e.Store.Get(request.Id)

	status := rmq.STATUS_OK
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			status = dto.STATUS_NOT_FOUND
		} else {
			status = rmq.STATUS_ERROR
		}
	}

	var replyStatus dto.EventStatusReply
	replyStatus.EventID = event.Id
	if event.IsTook {
		if event.IsWin {
			replyStatus.Status = dto.EVENT_STATUS_WIN
		} else {
			replyStatus.Status = dto.EVENT_STATUS_LOSS
		}
	} else {
		replyStatus.Status = dto.EVENT_STATUS_OPEN
	}

	log.Printf("GetEventStatus: request.Id: %d; eventId: %d; status: %s;", request.Id, event.Id, status)
	return replyStatus, status, err
}

var ErrInvalidCoefficient error = errors.New("coefficient must be in range (0, 1)")

func (e *EventService) handleCreateEvent(request dto.CreateEventRequest) (dto.Event, rmq.RPCResponseStatus, error) {

	if request.Coefficient.GreaterThanOrEqual(decimal.NewFromInt(1)) || request.Coefficient.LessThanOrEqual(decimal.NewFromInt(0)) {
		return dto.Event{}, rmq.STATUS_ERROR, ErrInvalidCoefficient
	}

	newEvent := dto.Event{
		Id:          int(e.lastEventId.Add(1)), // TODO: пока нет БД
		Coefficient: request.Coefficient,
		IsTook:      false,
		IsWin:       false,
	}
	e.Store.Put(newEvent.Id, newEvent)

	log.Printf("CreateEvent: eventId: %d; coefficient: %v;", newEvent.Id, newEvent.Coefficient)
	return newEvent, rmq.STATUS_OK, nil
}
