package internal

import (
	"errors"
	"fmt"
	"sort"
	"sync/atomic"

	"bookmaker/pkg/store"
	"bookmaker/shared/dto"

	"github.com/shopspring/decimal"
)

var (
	ErrInvalidCoefficient    = errors.New("coefficient must be in range (0, 1)")
	ErrEventCannotReopened   = errors.New("event cannot be reopened")
	ErrUnexpectedEventStatus = errors.New("unexpected event status")
	ErrInvalidEventsQuery    = errors.New("invalid events query")
)

type EventService struct {
	Store       store.Store[dto.Event]
	lastEventID atomic.Int64 // TODO: пока нет БД
}

func NewEventService(st store.Store[dto.Event]) *EventService {
	s := &EventService{Store: st}
	s.lastEventID.Store(9) // TODO: пока нет БД
	return s
}

func statusOf(event dto.Event) dto.EventStatus {
	if !event.IsTook {
		return dto.EVENT_STATUS_OPEN
	}
	if event.IsWin {
		return dto.EVENT_STATUS_WIN
	}
	return dto.EVENT_STATUS_LOSS
}

// GetEvents возвращает список событий с фильтрацией и пагинацией.
// Total — количество событий после фильтрации, но до пагинации.
func (s *EventService) GetEvents(
	status string,
	limit, offset int,
) ([]dto.Event, int, error) {
	switch status {
	case "", string(dto.EVENT_STATUS_OPEN),
		string(dto.EVENT_STATUS_WIN), string(dto.EVENT_STATUS_LOSS):
		// Допустимый фильтр.
	default:
		return nil, 0, fmt.Errorf(
			"%w: unsupported status %q",
			ErrInvalidEventsQuery,
			status,
		)
	}

	// Значения по умолчанию для запросов, в которых параметры не переданы.
	if limit == 0 {
		limit = 20
	}

	if limit < 1 || limit > 100 {
		return nil, 0, fmt.Errorf(
			"%w: limit must be between 1 and 100",
			ErrInvalidEventsQuery,
		)
	}

	if offset < 0 {
		return nil, 0, fmt.Errorf(
			"%w: offset must be non-negative",
			ErrInvalidEventsQuery,
		)
	}

	events, err := s.Store.FindBy(func(event dto.Event) bool {
		return status == "" || string(statusOf(event)) == status
	})
	if err != nil {
		return nil, 0, fmt.Errorf("find events: %w", err)
	}

	// MapStore хранит данные в map, поэтому порядок выборки не гарантирован.
	// Сортировка нужна, чтобы пагинация была стабильной.
	sort.Slice(events, func(i, j int) bool {
		return events[i].Id < events[j].Id
	})

	total := len(events)

	// Пустой результат должен сериализоваться как [], а не как null.
	if offset >= total {
		return []dto.Event{}, total, nil
	}

	end := offset + limit
	if end > total {
		end = total
	}

	return events[offset:end], total, nil
}

func (s *EventService) GetEvent(id int) (dto.Event, error) {
	event, err := s.Store.Get(id)
	if err != nil {
		return dto.Event{}, fmt.Errorf("get event %d: %w", id, err)
	}
	return event, nil
}

func (s *EventService) GetEventStatus(id int) (dto.EventStatusResponse, error) {
	event, err := s.GetEvent(id)
	if err != nil {
		return dto.EventStatusResponse{}, err
	}
	return dto.EventStatusResponse{EventID: event.Id, Status: statusOf(event)}, nil
}

func (s *EventService) CreateEvent(coefficient decimal.Decimal) (dto.Event, error) {
	if !coefficient.GreaterThan(decimal.Zero) || !coefficient.LessThan(decimal.NewFromInt(1)) {
		return dto.Event{}, ErrInvalidCoefficient
	}

	event := dto.Event{
		Id:          int(s.lastEventID.Add(1)),
		Coefficient: coefficient,
	}
	if err := s.Store.Put(event.Id, event); err != nil {
		return dto.Event{}, fmt.Errorf("put in store: %w", err)
	}
	return event, nil
}

func (s *EventService) SetEventStatus(id int, status dto.EventStatus) (dto.EventStatusResponse, error) {
	event, err := s.GetEvent(id)
	if err != nil {
		return dto.EventStatusResponse{}, err
	}
	if event.IsTook {
		return dto.EventStatusResponse{}, ErrEventCannotReopened
	}

	event.IsTook = true
	switch status {
	case dto.EVENT_STATUS_WIN:
		event.IsWin = true
	case dto.EVENT_STATUS_LOSS:
		event.IsWin = false
	case dto.EVENT_STATUS_OPEN:
		// изменение статуса на EVENT_STATUS_OPEN ничего не меняет
	default:
		return dto.EventStatusResponse{}, ErrUnexpectedEventStatus
	}

	if err := s.Store.Put(event.Id, event); err != nil {
		return dto.EventStatusResponse{}, fmt.Errorf("put in store: %w", err)
	}
	return dto.EventStatusResponse{EventID: event.Id, Status: status}, nil
}
