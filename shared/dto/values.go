package dto

import "bookmaker/pkg/rmq"

// методы микросервиса Bets
const (
	GET_BET    rmq.RPCMethodName = "get_bet"
	CREATE_BET rmq.RPCMethodName = "create_bet"
	GET_EVENT  rmq.RPCMethodName = "get_event"
)

// все возможные статусы rpc ответов
const (
	STATUS_OK           rmq.RPCResponseStatus = "ok"
	STATUS_NOT_FOUND    rmq.RPCResponseStatus = "not_found"
	STATUS_ERROR        rmq.RPCResponseStatus = "error"
	STATUS_EVENT_CLOSED rmq.RPCResponseStatus = "event_closed"
)
