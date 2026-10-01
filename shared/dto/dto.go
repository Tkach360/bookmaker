package dto

import (
	"encoding/json"
	"fmt"

	"github.com/shopspring/decimal"
)

type RequestAction string

var (
	GET_BET    RequestAction = "get_bet"
	CREATE_BET RequestAction = "create_bet"
	GET_EVENT  RequestAction = "get_event"
)

type Request struct {
	Action  RequestAction   `json:"action"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

func PackRequest(action RequestAction, payload any) ([]byte, error) {
	bPayload, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshaling payload json: %w", err)
	}

	request, err := json.Marshal(Request{
		Action:  action,
		Payload: bPayload,
	})
	if err != nil {
		return nil, fmt.Errorf("marshaling request json: %w", err)
	}
	return request, nil
}

type Response struct {
	Status ReplyStatus `json:"status"`
	Error  string      `json:"error,omitempty"`

	// добавлять поле типа ответа как action в запросе нет смысла,
	// так как тот кто спрашивает должен понимать какой ответ он ждет
	Payload json.RawMessage `json:"payload,omitempty"`
}

func PackResponse(payload any, status ReplyStatus, e error) ([]byte, error) {
	var bPayload json.RawMessage
	var err error

	if payload != nil {
		bPayload, err = json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("marshaling payload json: %w", err)
		}
	}

	var strError string
	if e != nil {
		strError = e.Error()
	}

	response, err := json.Marshal(Response{
		Status:  status,
		Error:   strError,
		Payload: bPayload,
	})

	if err != nil {
		return nil, fmt.Errorf("marshaling response json: %w", err)
	}

	return response, nil
}

type Bet struct {
	Id      int             `json:"id"`
	EventId int             `json:"event_id"`
	Amount  decimal.Decimal `json:"amount"`
	User    string          `json:"user"`
}

type GetBetRequest struct {
	Id int `json:"id"`
}

type ReplyStatus string

var (
	OK        ReplyStatus = "ok"
	NOT_FOUND ReplyStatus = "not_found"
	ERROR     ReplyStatus = "error"
)

type Event struct {
	Id          int             `json:"id"`
	Coefficient decimal.Decimal `json:"coefficient"`
	IsWin       bool            `json:"is_win"`  // результат событи, не имеет смысла пока IsTook = False
	IsTook      bool            `json:"is_took"` // произошло ли уже событие
}

type CreateBetRequest struct {
	UserID  int             `json:"user_id"`
	EventID int             `json:"event_id"`
	Amount  decimal.Decimal `json:"amount"`
}

type EventStatus string

var (
	EVENT_OPEN     EventStatus = "open"
	EVENT_CLOSED   EventStatus = "closed"
	EVENT_FINISHED EventStatus = "finished"
)

type EventStatusReply struct {
	EventID int         `json:"event_id"`
	Status  EventStatus `json:"status"`
}
