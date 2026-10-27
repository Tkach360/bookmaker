package internal

import (
	"errors"
	"testing"

	"bookmaker/pkg/store"
	"bookmaker/shared/dto"

	"github.com/shopspring/decimal"
)

func newTestService() *EventService {
	s := store.NewMapStore[dto.Event]()
	s.Put(1, dto.Event{Id: 1, Coefficient: decimal.NewFromFloat(0.6), IsWin: false, IsTook: false})
	s.Put(2, dto.Event{Id: 2, Coefficient: decimal.NewFromFloat(0.2), IsWin: true, IsTook: true})
	s.Put(3, dto.Event{Id: 3, Coefficient: decimal.NewFromFloat(0.9), IsWin: false, IsTook: true})

	return NewEventService(s)
}

func TestGetEvent(t *testing.T) {
	svc := newTestService()

	tests := []struct {
		name    string
		id      int
		wantErr error // nil - ошибки быть не должно
	}{
		{"событие найдено", 1, nil},
		{"событие не найдено", 99, store.ErrNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event, err := svc.GetEvent(tt.id)

			if !errors.Is(err, tt.wantErr) {
				t.Errorf("err = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr == nil && event.Id != tt.id {
				t.Errorf("event.Id = %d, want %d", event.Id, tt.id)
			}
		})
	}
}

func TestGetEventStatus(t *testing.T) {
	svc := newTestService()

	tests := []struct {
		name       string
		id         int
		wantStatus dto.EventStatus
		wantErr    error
	}{
		{"событие ещё не произошло", 1, dto.EVENT_STATUS_OPEN, nil},
		{"событие произошло, выиграло", 2, dto.EVENT_STATUS_WIN, nil},
		{"событие произошло, проиграло", 3, dto.EVENT_STATUS_LOSS, nil},
		{"событие не найдено", 99, "", store.ErrNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reply, err := svc.GetEventStatus(tt.id)

			if !errors.Is(err, tt.wantErr) {
				t.Errorf("err = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}
			if reply.EventID != tt.id || reply.Status != tt.wantStatus {
				t.Errorf("reply = %+v, want event %d with status %q", reply, tt.id, tt.wantStatus)
			}
		})
	}
}

// failingStore всегда возвращает ошибку, отличную от store.ErrNotFound.
// Остальные методы интерфейса не реализованы: при вызове будет паника,
// а тестируемым методам они не нужны.
type failingStore struct {
	store.Store[dto.Event]
}

func (failingStore) Get(int) (dto.Event, error) {
	return dto.Event{}, errors.New("storage unavailable")
}

func TestStoreError(t *testing.T) {
	svc := NewEventService(failingStore{})

	t.Run("GetEvent", func(t *testing.T) {
		_, err := svc.GetEvent(1)
		if err == nil || errors.Is(err, store.ErrNotFound) {
			t.Errorf("err = %v, want non-nil and not ErrNotFound", err)
		}
	})

	t.Run("GetEventStatus", func(t *testing.T) {
		_, err := svc.GetEventStatus(1)
		if err == nil || errors.Is(err, store.ErrNotFound) {
			t.Errorf("err = %v, want non-nil and not ErrNotFound", err)
		}
	})
}

func TestCreateEvent(t *testing.T) {
	svc := newTestService()

	tests := []struct {
		name        string
		coefficient decimal.Decimal
		wantErr     error
	}{
		{"событие создано", decimal.NewFromFloat(0.2), nil},
		{"событие не создано, коэф 1", decimal.NewFromFloat(1), ErrInvalidCoefficient},
		{"событие не создано, коэф 2", decimal.NewFromFloat(2), ErrInvalidCoefficient},
		{"событие не создано, коэф 0", decimal.NewFromFloat(0), ErrInvalidCoefficient},
		{"событие не создано, коэф -1", decimal.NewFromFloat(-1), ErrInvalidCoefficient},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event, err := svc.CreateEvent(tt.coefficient)

			if !errors.Is(err, tt.wantErr) {
				t.Errorf("err = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}
			if !event.Coefficient.Equal(tt.coefficient) {
				t.Errorf("event.Coefficient = %v, want %v", event.Coefficient, tt.coefficient)
			}

			stored, err := svc.Store.Get(event.Id)
			if err != nil {
				t.Fatalf("could not get event from store, err = %v", err)
			}
			if stored.Id != event.Id || stored.IsTook || stored.IsWin {
				t.Errorf("invalid event state in store: %+v", stored)
			}
		})
	}
}

func TestSetEventStatus(t *testing.T) {
	tests := []struct {
		name              string
		setupStore        func() store.Store[dto.Event]
		id                int
		status            dto.EventStatus
		wantErr           error
		checkStoreUpdated bool
	}{
		{
			name: "успешно: установка статуса WIN",
			setupStore: func() store.Store[dto.Event] {
				s := store.NewMapStore[dto.Event]()
				s.Put(1, dto.Event{Id: 1})
				return s
			},
			id: 1, status: dto.EVENT_STATUS_WIN, checkStoreUpdated: true,
		},
		{
			name: "успешно: установка статуса LOSS",
			setupStore: func() store.Store[dto.Event] {
				s := store.NewMapStore[dto.Event]()
				s.Put(2, dto.Event{Id: 2})
				return s
			},
			id: 2, status: dto.EVENT_STATUS_LOSS, checkStoreUpdated: true,
		},
		{
			name: "ошибка: событие не найдено",
			setupStore: func() store.Store[dto.Event] {
				return store.NewMapStore[dto.Event]()
			},
			id: 99, status: dto.EVENT_STATUS_WIN, wantErr: store.ErrNotFound,
		},
		{
			name: "ошибка: событие уже закрыто",
			setupStore: func() store.Store[dto.Event] {
				s := store.NewMapStore[dto.Event]()
				s.Put(3, dto.Event{Id: 3, IsTook: true})
				return s
			},
			id: 3, status: dto.EVENT_STATUS_WIN, wantErr: ErrEventCannotReopened,
		},
		{
			name: "ошибка: неожиданный статус",
			setupStore: func() store.Store[dto.Event] {
				s := store.NewMapStore[dto.Event]()
				s.Put(4, dto.Event{Id: 4})
				return s
			},
			id: 4, status: "INVALID_STATUS", wantErr: ErrUnexpectedEventStatus,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := NewEventService(tt.setupStore())

			reply, err := svc.SetEventStatus(tt.id, tt.status)

			if !errors.Is(err, tt.wantErr) {
				t.Errorf("err = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr == nil && (reply.EventID != tt.id || reply.Status != tt.status) {
				t.Errorf("reply = %+v, want event %d with status %q", reply, tt.id, tt.status)
			}

			if tt.checkStoreUpdated {
				updated, getErr := svc.Store.Get(tt.id)
				if getErr != nil {
					t.Fatalf("failed to get updated event from store: %v", getErr)
				}
				if !updated.IsTook {
					t.Error("expected event.IsTook to be true")
				}
				if wantWin := tt.status == dto.EVENT_STATUS_WIN; updated.IsWin != wantWin {
					t.Errorf("expected event.IsWin to be %v, got %v", wantWin, updated.IsWin)
				}
			}
		})
	}
}
