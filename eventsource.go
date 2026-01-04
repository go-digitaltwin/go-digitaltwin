package digitaltwin

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"reflect"

	"github.com/danielorbach/go-component"
	"gocloud.dev/pubsub"
)

// DigitalTwin provides methods for processing graph changes through event
// streams.
type DigitalTwin struct {
	Applier Applier
}

// Compiler is a function that transforms a GraphChanged notification into a
// Compilation that can be applied to a graph.
type Compiler func(GraphChanged) (Compilation, error)

// CompileChanges returns a component.Proc that subscribes to a pubsub
// subscription, decodes incoming GraphChanged messages, compiles them using
// the provided Compiler, and applies the resulting Compilation using the
// DigitalTwin's Applier.
func (d DigitalTwin) CompileChanges(sub *pubsub.Subscription, process Compiler) component.Proc {
	source := EventSource{
		subscription: sub,
		eventType:    reflect.TypeOf(GraphChanged{}),
		decoder: func(p []byte, v reflect.Value) error {
			return gob.NewDecoder(bytes.NewReader(p)).DecodeValue(v)
		},
	}
	return source.Stream(func(ctx context.Context, msg any) error {
		changed := msg.(GraphChanged)
		compilation, err := process(changed)
		if err != nil {
			return fmt.Errorf("compile: %w", err)
		}

		if err := d.Applier.Apply(ctx, compilation); err != nil {
			return fmt.Errorf("apply: %w", err)
		}
		return nil
	})
}

// EventSource wraps a pubsub subscription and decodes incoming messages into
// typed events.
type EventSource struct {
	subscription *pubsub.Subscription
	eventType    reflect.Type
	decoder      func(p []byte, v reflect.Value) error
}

// EventHandler is a function that processes a decoded event message.
type EventHandler func(ctx context.Context, msg any) error

// Stream returns a component.Proc that continuously receives messages from the
// subscription, decodes them using the configured decoder, and passes them to
// the provided EventHandler.
func (s EventSource) Stream(h EventHandler) component.Proc {
	return func(l *component.L) {
		for l.Continue() {
			msg, err := s.subscription.Receive(l.Context())
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
					// we're shutting down
					return
				}
				l.Fatal(fmt.Errorf("receive: %w", err))
			}
			// always ack, even if we fail to decode.
			// otherwise, we might get stuck processing
			// the same failed message
			msg.Ack()

			v := reflect.New(s.eventType)
			if err := s.decoder(msg.Body, v); err != nil {
				l.Fatal(fmt.Errorf("decode: %w", err))
			}

			if err := h(l.Context(), v.Elem().Interface()); err != nil {
				l.Fatal(fmt.Errorf("process: %w", err))
			}
		}
	}
}
