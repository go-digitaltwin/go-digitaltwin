package digitaltwin

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/danielorbach/go-component"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"gocloud.dev/pubsub"
	"golang.org/x/sync/errgroup"
)

// Register the graph component change types using gob.Register(). This is
// required to identify the type of change in the notified event after decoding
// it using gob.
func init() {
	gob.Register(AssemblyCreated{})
	gob.Register(AssemblyUpdated{})
	gob.Register(AssemblyRemoved{})
}

type disassembler struct {
	graphName string
	source    *pubsub.Subscription
	sink      *pubsub.Topic
}

// NewDisassembler returns a [component.Procedure] that disassembles a digital
// twin's entire graph change notifications (received from the given source) into
// individual component graph change notifications and publishes them to the
// specified sink.
//
// It consumes digitaltwin.GraphChanged notifications and produces
// digitaltwin.ComponentChanged notifications.
//
// The disassembler measures the duration of processing each graph change
// notification and labels each measurement record with the provided graph name
// (e.g. "assettwin").
func NewDisassembler(graphName string, source *pubsub.Subscription, sink *pubsub.Topic) component.Procedure {
	return disassembler{
		graphName: graphName,
		source:    source,
		sink:      sink,
	}
}

func (d disassembler) Exec(l *component.L) {
	logger := component.Logger(l.Context())
	for l.Continue() {
		msg, err := d.source.Receive(l.GraceContext())
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				return
			}

			// Based on the pubsub Receive function documentation, if Receive returns an
			// error, it is either a non-retryable error from the underlying driver or
			// indicates that the provided context is Done. In case of a non-retryable error,
			// we should either recreate the Subscription or exit. Since we currently lack
			// the mechanism to recreate the target Subscription, we opt to trigger a process
			// shutdown. This block uses a panic to terminate operations as there is no
			// existing mechanism to stop the parent component lifecycle. A more refined
			// solution for recreating the target Subscription will be implemented in the
			// component v2.
			panic("cannot receive messages from the pubsub service")
		}

		err = d.handleMessage(l.GraceContext(), logger, msg)
		if err != nil {
			// According to the service requirements, the service shall never proceed to
			// decompose additional graph changes before publishing messages about all
			// components in the previous message. Therefore, if handleMessage fails for any
			// reason, we initiate a process shutdown. The service will then continuously
			// attempt to process the same message until it succeeds.
			logger.Error("Couldn't handle GraphChanged message",
				slog.Any("error", err),
			)
			panic("cannot proceed to the next GraphChanged message due to failure")
		}

		// Acknowledge the message only if the handling process is fully successful, as
		// the service maintains an at-least-once delivery constraint.
		msg.Ack()
	}
}

// handleMessage handles a GraphChanged message by disassembling it into
// ComponentChanged messages and publishing each message to the relevant aspect.
// It returns an error if it fails to publish even a single ComponentChanged
// message.
func (d disassembler) handleMessage(ctx context.Context, logger *slog.Logger, msg *pubsub.Message) (err error) {
	ctx, span := tracer.Start(ctx, "disassembler.handleMessage", trace.WithAttributes(
		attribute.String("msg.id", msg.LoggableID),
	))
	defer span.End()

	defer func(start time.Time) {
		success := err == nil
		elapsed := time.Since(start)
		measureDisassembly(ctx, d.graphName, success, elapsed)
	}(time.Now())

	logger.Debug("New GraphChanged message received, starting message handling...")
	logger.Debug("Decoding message using gob...")
	var changed GraphChanged
	if err := gob.NewDecoder(bytes.NewReader(msg.Body)).Decode(&changed); err != nil {
		err := fmt.Errorf("decode gob: %w", err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	if changed.IsEmpty() {
		// As noted in the IsEmpty() documentation, it returns true when the graph hash
		// before and after are the same, indicating no changes in the graph. In this
		// case, we put only the graph hash before as attribute on the log, as it is
		// identical to the graph hash after.
		logger.Info("There are no changes in the GraphChanged message, message skipped", slog.Any("graph-hash", changed.GraphBefore))
		return nil
	}

	logger = logger.With(
		slog.Any("graph-before-hash", changed.GraphBefore),
		slog.Any("graph-after-hash", changed.GraphAfter),
	)
	logger.Debug("Disassembling graph change into graph component changes...")
	componentsChanges := disassembleGraph(changed)

	g, ctx := errgroup.WithContext(ctx)
	for _, c := range componentsChanges {
		c := c
		g.Go(func() error {
			return d.notifyChange(ctx, logger, c)
		})
	}

	// Ensures that any goroutines started by the error group are allowed to finish
	// and that their errors are handled before the function can return, thus
	// maintaining robust error tracking.
	if err := g.Wait(); err != nil {
		return fmt.Errorf("send component changes: %w", err)
	}
	logger.Info("GraphChanged message handled successfully")

	return nil
}

func (d disassembler) notifyChange(ctx context.Context, logger *slog.Logger, c ComponentChanged) error {
	ctx, span := tracer.Start(ctx, "disassembler.handleMessage", trace.WithAttributes(
		attribute.Stringer("graph.hash", c.GraphHash),
		attribute.Stringer("component.id", c.AssemblyHash()),
	))
	defer span.End()

	logger = logger.With(
		slog.Any("component-id", c.AssemblyID()),
	)
	logger.Debug("Encoding ComponentChanged message using gob...")
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	if err := enc.Encode(c); err != nil {
		err := fmt.Errorf("encode gob: %w", err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	logger.Debug("Sending ComponentChanged message...")
	// To ensure ordered message delivery with the Kafka messaging broker, messages
	// can be produced with a key. Kafka guarantees that messages with the same key
	// are written to the same topic partition. As consumers read messages in order
	// from each partition, the message ordering is preserved.
	//
	// The componentID is included as metadata on the message to enable key-based
	// partitioning in Kafka. Please note that there is no one-to-one relationship
	// between a partition and a key. A partition can include multiple keys, but it
	// does guarantee that a consumer of a specific partition will consume messages
	// with the same key in the correct order.
	//
	// This ability will be used when consuming the ComponentChanged messages from
	// the same topic using multiple consumers.
	msg := &pubsub.Message{Body: b.Bytes(), Metadata: map[string]string{"componentID": c.AssemblyID().String()}}
	if err := d.sink.Send(ctx, msg); err != nil {
		err := fmt.Errorf("send: %w", err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	logger.Debug("ComponentChanged message sent successfully")

	return nil
}

// ComponentChanged notifies about changes to a specific component in the
// internal graph-based world-view maintained by a digital twin. The changes can
// be:
//   - A new assembly is created.
//   - An existing assembly is updated.
//   - An existing assembly is deleted.
//
// Use IsCreated, IsUpdated, and IsRemoved to identify the type of change.
//
// IMPORTANT: Before encoding, register the type that implements the Assembly
// interface (AssemblyCreated, AssemblyUpdated, AssemblyRemoved) using
// gob.Register(). This is critical to identify the change type of the notified
// event.
type ComponentChanged struct {
	Assembly
	// GraphHash represents the hash of the entire graph at the time when the
	// specific assembly was changed. It corresponds to the GraphChanged.GraphAfter
	// field of the GraphChanged message that this component change is a part of.
	GraphHash ForestHash
	// The time, in UTC, the entire graph change was computed. The information in
	// this message is accurate up to this timestamp, not a moment afterward.
	Timestamp time.Time
}

// IsCreated returns true if a new assembly is created.
func (c ComponentChanged) IsCreated() bool {
	if _, ok := c.Assembly.(AssemblyCreated); ok {
		return true
	}
	return false
}

// IsUpdated returns true if an existing assembly is updated.
func (c ComponentChanged) IsUpdated() bool {
	if _, ok := c.Assembly.(AssemblyUpdated); ok {
		return true
	}
	return false
}

// IsRemoved returns true if an existing assembly is removed.
func (c ComponentChanged) IsRemoved() bool {
	if _, ok := c.Assembly.(AssemblyRemoved); ok {
		return true
	}
	return false
}

// disassembleGraph disassembles the provided GraphChanged message into
// individual ComponentChanged messages, one for each graph component change
// (AssemblyCreated, AssemblyUpdated, AssemblyRemoved). It returns a slice of
// ComponentChanged messages.
func disassembleGraph(graph GraphChanged) (changes []ComponentChanged) {
	for _, c := range graph.Created {
		changes = append(changes, ComponentChanged{
			Assembly:  c,
			GraphHash: graph.GraphAfter,
			Timestamp: graph.Timestamp,
		})
	}

	for _, c := range graph.Updated {
		changes = append(changes, ComponentChanged{
			Assembly:  c,
			GraphHash: graph.GraphAfter,
			Timestamp: graph.Timestamp,
		})
	}

	for _, c := range graph.Removed {
		changes = append(changes, ComponentChanged{
			Assembly:  c,
			GraphHash: graph.GraphAfter,
			Timestamp: graph.Timestamp,
		})
	}

	return changes
}
