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
	Id int `json:"event_id"`
}

type CreateEventRequest struct {
	Coefficient decimal.Decimal `json:"coefficient"`
}

type CreateBetRequest struct {
	UserID  int             `json:"user_id"`
	EventID int             `json:"event_id"`
	Amount  decimal.Decimal `json:"amount"`
}

type EventStatus string

var (
	EVENT_STATUS_OPEN EventStatus = "open" // когда событие ещё не произошло
	EVENT_STATUS_WIN  EventStatus = "win"
	EVENT_STATUS_LOSS EventStatus = "loss"
)

type SetEventStatusRequest struct {
	Id     int         `json:"event_id"`
	Status EventStatus `json:"status"`
}

type EventStatusReply struct {
	EventID int         `json:"event_id"`
	Status  EventStatus `json:"status"`
}
