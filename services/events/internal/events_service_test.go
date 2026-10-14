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
