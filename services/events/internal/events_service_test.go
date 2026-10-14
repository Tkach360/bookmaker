package internal

import (
	"bookmaker/pkg/rmq"
	"bookmaker/pkg/store"
	"bookmaker/shared/dto"
	"encoding/json"
	"errors"
	"testing"

	"github.com/shopspring/decimal"
)

func newTestService() *EventService {
	s := store.NewMapStore[dto.Event]()
	s.Put(1, dto.Event{Id: 1, Coefficient: decimal.NewFromFloat(0.6), IsWin: false, IsTook: false})
	s.Put(2, dto.Event{Id: 2, Coefficient: decimal.NewFromFloat(0.2), IsWin: true, IsTook: true})
	s.Put(3, dto.Event{Id: 3, Coefficient: decimal.NewFromFloat(0.9), IsWin: false, IsTook: true})

	return &EventService{Store: s}
}

func TestHandleGetEvent(t *testing.T) {
	svc := newTestService()

	tests := []struct {
		name       string
		id         int
		wantStatus rmq.RPCResponseStatus
		wantErr    error // nil - ошибки быть не должно
	}{
		{"событие найдено", 1, rmq.STATUS_OK, nil},
		{"событие не найдено", 99, dto.STATUS_NOT_FOUND, store.ErrNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event, status, err := svc.handleGetEvent(dto.GetEventRequest{Id: tt.id})

			if status != tt.wantStatus {
				t.Errorf("status = %q, want %q", status, tt.wantStatus)
			}
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("err = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr == nil && event.Id != tt.id {
				t.Errorf("event.Id = %d, want %d", event.Id, tt.id)
			}
		})
	}
}

func TestHandleGetEventStatus(t *testing.T) {
	svc := newTestService()

	tests := []struct {
		name            string
		id              int
		wantEventStatus dto.EventStatus
		wantStatus      rmq.RPCResponseStatus
		wantErr         error
	}{
		{"событие ещё не произошло", 1, dto.EVENT_STATUS_OPEN, rmq.STATUS_OK, nil},
		{"событие произошло, выиграло", 2, dto.EVENT_STATUS_WIN, rmq.STATUS_OK, nil},
		{"событие произошло, проиграло", 3, dto.EVENT_STATUS_LOSS, rmq.STATUS_OK, nil},
		{"событие не найдено", 99, "", dto.STATUS_NOT_FOUND, store.ErrNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reply, status, err := svc.handleGetEventStatus(dto.GetEventRequest{Id: tt.id})

			if status != tt.wantStatus {
				t.Errorf("status = %q, want %q", status, tt.wantStatus)
			}
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("err = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				return // при ошибке содержимое ответа не проверяем: клиент его не получит
			}
			if reply.EventID != tt.id {
				t.Errorf("reply.EventID = %d, want %d", reply.EventID, tt.id)
			}
			if reply.Status != tt.wantEventStatus {
				t.Errorf("reply.Status = %q, want %q", reply.Status, tt.wantEventStatus)
			}
		})
	}
}

// failingStore всегда возвращает ошибку, отличную от store.ErrNotFound
// Остальные методы интерфейса не реализованы: при вызове будет паника,
// а обработчикам они не нужны
type failingStore struct {
	store.Store[dto.Event]
}

func (failingStore) Get(int) (dto.Event, error) {
	return dto.Event{}, errors.New("storage unavailable")
}

func TestHandlers_StoreError(t *testing.T) {
	svc := &EventService{Store: failingStore{}}

	t.Run("GetEvent", func(t *testing.T) {
		_, status, err := svc.handleGetEvent(dto.GetEventRequest{Id: 1})

		if status != rmq.STATUS_ERROR {
			t.Errorf("status = %q, want %q", status, rmq.STATUS_ERROR)
		}
		if err == nil || errors.Is(err, store.ErrNotFound) {
			t.Errorf("err = %v, want non-nil and not ErrNotFound", err)
		}
	})

	t.Run("GetEventStatus", func(t *testing.T) {
		_, status, err := svc.handleGetEventStatus(dto.GetEventRequest{Id: 1})

		if status != rmq.STATUS_ERROR {
			t.Errorf("status = %q, want %q", status, rmq.STATUS_ERROR)
		}
		if err == nil || errors.Is(err, store.ErrNotFound) {
			t.Errorf("err = %v, want non-nil and not ErrNotFound", err)
		}
	})
}

// Проверяет формат на проводе JSON-запрос -> обработчик -> JSON-ответ RPCResponse
// Именно здесь ловится несовпадение имён полей между клиентом и сервером
// при неверном теге id придет нулевой и событие "не найдется"
func TestGetEventStatus_WireFormat(t *testing.T) {
	handler := rmq.HandlerWrap(newTestService().handleGetEventStatus)

	raw, err := handler([]byte(`{"event_id":2}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var resp rmq.RPCResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("response is not valid RPCResponse json: %v", err)
	}
	if resp.Err != "" || resp.Status != rmq.STATUS_OK {
		t.Fatalf("resp = {err: %q, status: %q}, want empty error and %q", resp.Err, resp.Status, rmq.STATUS_OK)
	}

	var reply dto.EventStatusReply
	if err := json.Unmarshal(resp.Payload, &reply); err != nil {
		t.Fatalf("payload is not valid EventStatusReply json: %v", err)
	}
	if reply.EventID != 2 || reply.Status != dto.EVENT_STATUS_WIN {
		t.Errorf("reply = %+v, want event 2 with status %q", reply, dto.EVENT_STATUS_WIN)
	}
}

// Ошибка сервиса не должна теряться: клиент определяет "не найдено" по статусу ответа.
func TestGetEventStatus_WireFormatNotFound(t *testing.T) {
	handler := rmq.HandlerWrap(newTestService().handleGetEventStatus)

	raw, err := handler([]byte(`{"id":99}`))
	if err != nil {
		t.Fatalf("unexpected transport error: %v", err)
	}

	var resp rmq.RPCResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("response is not valid RPCResponse json: %v", err)
	}
	if resp.Status != dto.STATUS_NOT_FOUND {
		t.Errorf("status = %q, want %q", resp.Status, dto.STATUS_NOT_FOUND)
	}
	if resp.Err == "" {
		t.Error("expected non-empty error in response")
	}
}

func TestGetEventStatus_BadRequest(t *testing.T) {
	handler := rmq.HandlerWrap(newTestService().handleGetEventStatus)

	if _, err := handler([]byte(`not json`)); err == nil {
		t.Error("expected error for malformed request body")
	}
}

func TestHandleCreateEvent(t *testing.T) {
	svc := newTestService()

	tests := []struct {
		name        string
		coefficient decimal.Decimal
		wantStatus  rmq.RPCResponseStatus
		wantErr     error // nil - ошибки быть не должно
	}{
		{"событие создано", decimal.NewFromFloat(0.2), rmq.STATUS_OK, nil},
		{"событие не создано, коэф 1", decimal.NewFromFloat(1), rmq.STATUS_ERROR, ErrInvalidCoefficient},
		{"событие не создано, коэф 2", decimal.NewFromFloat(2), rmq.STATUS_ERROR, ErrInvalidCoefficient},
		{"событие не создано, коэф 0", decimal.NewFromFloat(0), rmq.STATUS_ERROR, ErrInvalidCoefficient},
		{"событие не созданоб коэф -1", decimal.NewFromFloat(-1), rmq.STATUS_ERROR, ErrInvalidCoefficient},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event, status, err := svc.handleCreateEvent(dto.CreateEventRequest{Coefficient: tt.coefficient})

			if status != tt.wantStatus {
				t.Errorf("status = %q, want %q", status, tt.wantStatus)
			}
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("err = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr == nil && event.Coefficient != tt.coefficient {
				t.Errorf("event.Coefficient = %d, want %d", event.Coefficient, tt.coefficient)
			}

			if tt.wantErr == nil {
				storeEvent, err := svc.Store.Get(event.Id)
				if err != nil {
					t.Errorf("could not get event from store, err = %v", err)
				}

				if storeEvent.Id != event.Id || storeEvent.IsTook != false || storeEvent.IsWin != false {
					t.Errorf("invalid event state in store, id = %d, IsTook = %v, IsWin = %v", storeEvent.Id, storeEvent.IsTook, storeEvent.IsWin)
				}
			}
		})
	}
}

func TestHandleSetEventStatus(t *testing.T) {
	tests := []struct {
		name              string
		setupStore        func() store.Store[dto.Event]
		request           dto.SetEventStatusRequest
		wantStatus        rmq.RPCResponseStatus
		wantErr           error
		wantReplyStatus   dto.EventStatus
		checkStoreUpdated bool
	}{
		{
			name: "успешно: установка статуса WIN",
			setupStore: func() store.Store[dto.Event] {
				s := store.NewMapStore[dto.Event]()
				s.Put(1, dto.Event{Id: 1, IsTook: false, IsWin: false})
				return s
			},
			request:           dto.SetEventStatusRequest{Id: 1, Status: dto.EVENT_STATUS_WIN},
			wantStatus:        rmq.STATUS_OK,
			wantErr:           nil,
			wantReplyStatus:   dto.EVENT_STATUS_WIN,
			checkStoreUpdated: true,
		},
		{
			name: "успешно: установка статуса LOSS",
			setupStore: func() store.Store[dto.Event] {
				s := store.NewMapStore[dto.Event]()
				s.Put(2, dto.Event{Id: 2, IsTook: false, IsWin: false})
				return s
			},
			request:           dto.SetEventStatusRequest{Id: 2, Status: dto.EVENT_STATUS_LOSS},
			wantStatus:        rmq.STATUS_OK,
			wantErr:           nil,
			wantReplyStatus:   dto.EVENT_STATUS_LOSS,
			checkStoreUpdated: true,
		},
		{
			name: "ошибка: событие не найдено",
			setupStore: func() store.Store[dto.Event] {
				return store.NewMapStore[dto.Event]()
			},
			request:    dto.SetEventStatusRequest{Id: 99, Status: dto.EVENT_STATUS_WIN},
			wantStatus: dto.STATUS_NOT_FOUND,
			wantErr:    nil, // В исходном коде ошибка не возвращается, только статус
		},
		{
			name: "ошибка: событие уже закрыто",
			setupStore: func() store.Store[dto.Event] {
				s := store.NewMapStore[dto.Event]()
				s.Put(3, dto.Event{Id: 3, IsTook: true})
				return s
			},
			request:    dto.SetEventStatusRequest{Id: 3, Status: dto.EVENT_STATUS_WIN},
			wantStatus: rmq.STATUS_ERROR,
			wantErr:    ErrEventCannotReopened,
		},
		{
			name: "ошибка: неожиданный статус",
			setupStore: func() store.Store[dto.Event] {
				s := store.NewMapStore[dto.Event]()
				s.Put(4, dto.Event{Id: 4, IsTook: false})
				return s
			},
			request:    dto.SetEventStatusRequest{Id: 4, Status: "INVALID_STATUS"},
			wantStatus: rmq.STATUS_ERROR,
			wantErr:    ErrUnexpectedEventStatus,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &EventService{Store: tt.setupStore()}

			reply, status, err := svc.handleSetEventStatus(tt.request)

			if status != tt.wantStatus {
				t.Errorf("status = %q, want %q", status, tt.wantStatus)
			}
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("err = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr == nil && reply.Status != tt.wantReplyStatus {
				t.Errorf("reply.Status = %q, want %q", reply.Status, tt.wantReplyStatus)
			}

			if tt.checkStoreUpdated {
				updatedEvent, getErr := svc.Store.Get(tt.request.Id)
				if getErr != nil {
					t.Fatalf("failed to get updated event from store: %v", getErr)
				}
				if !updatedEvent.IsTook {
					t.Errorf("expected event.IsTook to be true")
				}
				expectedWin := tt.request.Status == dto.EVENT_STATUS_WIN
				if updatedEvent.IsWin != expectedWin {
					t.Errorf("expected event.IsWin to be %v, got %v", expectedWin, updatedEvent.IsWin)
				}
			}
		})
	}
}
