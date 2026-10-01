package internal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"bookmaker/pkg/broker"
	"bookmaker/shared/dto"
)

type EventClient struct {
	Broker       *broker.Broker
	RequestQueue string
	ReplyQueue   string
}

func NewEventClient(ctx context.Context, broker *broker.Broker, requestQueue, replyQueue string) *EventClient {
	if err := broker.ConnectPublishers(ctx, requestQueue); err != nil {
		log.Fatalf("connect events publisher: %v", err)
	}

	if err := broker.ConnectConsumers(ctx, replyQueue); err != nil {
		log.Fatalf("connect events consumer: %v", err)
	}

	go func() {
		if err := broker.ResponceListener(ctx, replyQueue); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("Events response listener exited with error: %v", err)
		}
	}()

	return &EventClient{
		Broker:       broker,
		RequestQueue: requestQueue,
		ReplyQueue:   replyQueue,
	}
}

func (c *EventClient) GetEventStatus(ctx context.Context, eventID int) (dto.EventStatus, error) {
	reqData := map[string]int{"event_id": eventID}

	requestBytes, err := dto.PackRequest(dto.GET_EVENT, reqData)
	if err != nil {
		return "", fmt.Errorf("pack get event request: %w", err)
	}

	respCh, err := c.Broker.Publish(ctx, c.RequestQueue, c.ReplyQueue, requestBytes)
	if err != nil {
		return "", fmt.Errorf("publish msg to events: %w", err)
	}

	select {
	case resp := <-respCh:
		if resp.Err != nil {
			return "", fmt.Errorf("rpc error from events: %w", resp.Err)
		}

		var reply dto.Response
		if err := json.Unmarshal(resp.Body, &reply); err != nil {
			return "", fmt.Errorf("unmarshaling events reply json: %w", err)
		}

		if reply.Status == dto.ERROR {
			return "", fmt.Errorf("events service error: %s", reply.Error)
		}
		if reply.Status == dto.NOT_FOUND {
			return "", fmt.Errorf("event with id %d not found", eventID)
		}

		if reply.Status == dto.OK && reply.Payload != nil {
			var eventData dto.EventStatusReply
			if err := json.Unmarshal(reply.Payload, &eventData); err != nil {
				return "", fmt.Errorf("unmarshaling event status data: %w", err)
			}

			return eventData.Status, nil
		}

		return "", fmt.Errorf("unexpected events reply status: %s", reply.Status)

	case <-ctx.Done():
		return "", ctx.Err()
	}
}
