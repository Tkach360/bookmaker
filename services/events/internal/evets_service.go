package internal

import (
	"context"

	"bookmaker/pkg/rmq"
	"bookmaker/shared/dto"
)

type EventService struct {
	Server *rmq.RPCServer
}

// устанавливает обработчики и запускает сервис в отдельной горутине
func (s *EventService) Start(ctx context.Context, errCh chan<- error) error {
	rmq.RegisterHandler(s.Server, dto.GET_EVENT, s.handleGetEvent)
	return s.Server.Start(ctx, errCh)
}

func (s *EventService) handleGetEvent(request dto.GetEventRequest) (dto.EventStatusReply, rmq.RPCResponseStatus, error) {
	// TODO: сейчас тут просто заглушка, все события открыты
	return dto.EventStatusReply{
		EventID: request.Id,
		Status:  dto.EVENT_OPEN,
	}, dto.STATUS_OK, nil
}
