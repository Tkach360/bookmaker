package internal

import (
	"context"
	"errors"
	"log"

	"bookmaker/pkg/rmq"
	"bookmaker/pkg/store"
	"bookmaker/shared/dto"
)

type EventService struct {
	Store  store.Store[dto.Event]
	Server *rmq.RPCServer
}

// устанавливает обработчики и запускает сервис в отдельной горутине
func (e *EventService) Start(ctx context.Context, errCh chan<- error) error {
	rmq.RegisterHandler(e.Server, dto.GET_EVENT, e.handleGetEvent)
	rmq.RegisterHandler(e.Server, dto.GET_EVENT_STATUS, e.handleGetEventStatus)
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
