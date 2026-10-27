// Package genamqp adapts generated AsyncAPI code to the project's RabbitMQ
// conventions. Generated files remain untouched.
package genamqp

import (
	"context"
	"errors"
	"fmt"
	"time"

	"bookmaker/proto/amqp"

	"github.com/bdragon300/go-asyncapi/run"
)

const (
	HeaderMessageType   = "messageType"
	HeaderCorrelationID = "correlationId"
	HeaderReplyTo       = "replyTo"

	DefaultHandlerTimeout = 5 * time.Second
)

var ErrUnknownMessageType = errors.New("unknown message type")

// Meta contains the routing and correlation headers of an incoming message.
type Meta struct {
	MessageType   string
	CorrelationID string
	ReplyTo       string
}

func MetaFrom(headers run.Headers) Meta {
	return Meta{
		MessageType:   headerString(headers[HeaderMessageType]),
		CorrelationID: headerString(headers[HeaderCorrelationID]),
		ReplyTo:       headerString(headers[HeaderReplyTo]),
	}
}

func headerString(value any) string {
	switch value := value.(type) {
	case string:
		return value
	case []byte:
		return string(value)
	default:
		return ""
	}
}

// Headers are written using the names and scalar values from the AsyncAPI
// contract, rather than the generated code's PascalCase keys and *string values.
type Headers struct {
	MessageType   string
	CorrelationID string
	ReplyTo       string
}

func (h Headers) table() run.Headers {
	result := run.Headers{
		HeaderMessageType:   h.MessageType,
		HeaderCorrelationID: h.CorrelationID,
	}
	if h.ReplyTo != "" {
		result[HeaderReplyTo] = h.ReplyTo
	}
	return result
}

// Publishing is implemented by generated channel types.
type Publishing interface {
	Queue() string
	Publish(context.Context, ...amqp.EnvelopeWriter) error
}

// Publish uses the generated sealer for the payload, then normalizes headers
// and sets the queue name as routing key for channels bound as RabbitMQ queues.
func Publish(
	ctx context.Context,
	channel Publishing,
	seal func(amqp.EnvelopeWriter) error,
	headers Headers,
) error {
	envelope := amqp.NewEnvelopeOut(nil)
	if err := seal(envelope); err != nil {
		return fmt.Errorf("seal %s: %w", headers.MessageType, err)
	}
	envelope.SetHeaders(headers.table())
	envelope.SetRoutingKey(channel.Queue())
	return channel.Publish(ctx, envelope)
}

type Handler func(context.Context, amqp.EnvelopeReader, Meta) error

type Dispatcher struct {
	OnError        func(error)
	HandlerTimeout time.Duration
	handlers       map[string]Handler
}

func NewDispatcher(onError func(error)) *Dispatcher {
	return &Dispatcher{
		OnError:        onError,
		HandlerTimeout: DefaultHandlerTimeout,
		handlers:       make(map[string]Handler),
	}
}

// Handle registers a handler before Run starts.
func (d *Dispatcher) Handle(messageType string, handler Handler) {
	d.handlers[messageType] = handler
}

// Run consumes a queue through one generated Subscribe method and dispatches
// each message by its messageType header.
func (d *Dispatcher) Run(
	ctx context.Context,
	subscribe func(context.Context, func(amqp.EnvelopeReader)) error,
) error {
	return subscribe(ctx, func(envelope amqp.EnvelopeReader) {
		d.Dispatch(ctx, envelope)
	})
}

// Dispatch acknowledges every delivery, including unknown or invalid message
// types, so a poison message cannot be redelivered forever.
func (d *Dispatcher) Dispatch(ctx context.Context, envelope amqp.EnvelopeReader) {
	defer func() {
		if err := envelope.Ack(); err != nil {
			d.report(fmt.Errorf("ack: %w", err))
		}
	}()

	meta := MetaFrom(envelope.Headers())
	handler, ok := d.handlers[meta.MessageType]
	if !ok {
		d.report(fmt.Errorf("%w: %q", ErrUnknownMessageType, meta.MessageType))
		return
	}

	timeout := d.HandlerTimeout
	if timeout <= 0 {
		timeout = DefaultHandlerTimeout
	}
	handlerCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()

	if err := callHandler(handlerCtx, handler, envelope, meta); err != nil {
		d.report(fmt.Errorf("%s: %w", meta.MessageType, err))
	}
}

func (d *Dispatcher) report(err error) {
	if d.OnError != nil {
		d.OnError(err)
	}
}

func callHandler(ctx context.Context, handler Handler, envelope amqp.EnvelopeReader, meta Meta) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("handler panic: %v", recovered)
		}
	}()
	return handler(ctx, envelope, meta)
}
