package dto

import (
	"github.com/shopspring/decimal"
)

type Bet struct {
	Id      int             `json:"id"`
	EventId int             `json:"event_id"`
	Amount  decimal.Decimal `json:"amount"`
	User    string          `json:"user"`
}

type GetBetRequest struct {
	Id int `json:"id"`
}

type Event struct {
	Id          int             `json:"id"`
	Coefficient decimal.Decimal `json:"coefficient"`
	IsWin       bool            `json:"is_win"`  // результат событи, не имеет смысла пока IsTook = False
	IsTook      bool            `json:"is_took"` // произошло ли уже событие
}

type GetEventRequest struct {
	Id int `json:"id"`
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
