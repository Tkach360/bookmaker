package dto

import "bookmaker/pkg/rmq"

// методы микросервиса Bets
const (
	// bets
	GET_BET    rmq.RPCMethodName = "get_bet"
	CREATE_BET rmq.RPCMethodName = "create_bet"

	//events
	GET_EVENT        rmq.RPCMethodName = "get_event"
	GET_EVENT_STATUS rmq.RPCMethodName = "get_event_status"
	CREATE_EVENT     rmq.RPCMethodName = "create_event"
)

// возможные статусы rpc ответов, соответствующие логике работы приложения
const (
	STATUS_NOT_FOUND    rmq.RPCResponseStatus = "not_found"
	STATUS_EVENT_CLOSED rmq.RPCResponseStatus = "event_closed"
)
