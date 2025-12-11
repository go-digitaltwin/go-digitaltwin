package compilation

import (
	"bytes"
	"context"
	"encoding/gob"
	"iter"
	"reflect"

	"github.com/go-digitaltwin/go-digitaltwin"
	"github.com/go-digitaltwin/go-digitaltwin/assert"
)

// We register all Step implementations with gob.Register to enable serialisation
// and deserialisation across process boundaries.
//
// This registration is essential for the distributed compilation workflow,
// allowing steps to be transmitted between different environments and processes.
//
// Without this registration, the gob encoder would fail when attempting to
// serialise these types.
func init() {
	gob.Register(assertNode{})
	gob.Register(retractNode{})
	gob.Register(assertEdge{})
	gob.Register(retractEdges{})
	gob.Register(assertOneToOne{})
	gob.Register(assertOneToMany{})
	gob.Register(assertManyToOne{})
	gob.Register(assertManyToMany{})
}

// An assertNode is a Step that ensures a specific node exists in the graph.
type assertNode struct {
	Node digitaltwin.Value
}

func (s assertNode) Do(ctx context.Context, w digitaltwin.GraphWriter) error {
	return w.AssertNode(ctx, s.Node)
}

func (s assertNode) Targets() iter.Seq[digitaltwin.Value] {
	return func(yield func(digitaltwin.Value) bool) {
		if !yield(s.Node) {
			return
		}
	}
}

// A retractNode is a Step that removes a specific node from the graph along with
// all its connected edges.
type retractNode struct {
	Node digitaltwin.Value
}

func (s retractNode) Do(ctx context.Context, w digitaltwin.GraphWriter) error {
	return w.RetractNode(ctx, s.Node)
}

func (s retractNode) Targets() iter.Seq[digitaltwin.Value] {
	return func(yield func(digitaltwin.Value) bool) {
		if !yield(s.Node) {
			return
		}
	}
}

// An assertEdge is a Step that creates a directed relationship between two nodes
// in the graph.
type assertEdge struct {
	From, To digitaltwin.Value
}

func (s assertEdge) Do(ctx context.Context, w digitaltwin.GraphWriter) error {
	return w.AssertEdge(ctx, s.From, s.To)
}

func (s assertEdge) Targets() iter.Seq[digitaltwin.Value] {
	return func(yield func(digitaltwin.Value) bool) {
		if !yield(s.From) {
			return
		}
		if !yield(s.To) {
			return
		}
	}
}

// A retractEdges is a Step that performs a bulk removal of outgoing
// relationships from a node to nodes of a specific type.
type retractEdges struct {
	Node digitaltwin.Value
	Kind reflect.Type
}

func (s retractEdges) GobEncode() ([]byte, error) {
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	if err := enc.Encode(&s.Node); err != nil {
		return nil, err
	}
	sentinel := reflect.Zero(s.Kind).Interface()
	if err := enc.Encode(&sentinel); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (s *retractEdges) GobDecode(data []byte) error {
	dec := gob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&s.Node); err != nil {
		return err
	}
	var sentinel digitaltwin.Value
	if err := dec.Decode(&sentinel); err != nil {
		return err
	}
	s.Kind = reflect.TypeOf(sentinel)
	return nil
}

func (s retractEdges) Do(ctx context.Context, w digitaltwin.GraphWriter) error {
	// GraphWriter.RetractEdges returns the count of edges actually retracted when
	// called. Retracting an unexpected number of edges indicates the underlying
	// graph had become invalid. When we say "invalid" we mean that the specific
	// graph (nodes and their edges) should not have been created while the system
	// operates as expected. The system is designed to always maintain the same
	// relationships between the same node types. For example, maintaining one-to-one
	// relationships between nodes of type IMEI and IMSI.
	//
	// TODO (@danielorbach): now that we record the intention to perform a call to
	// `GraphWriter.RetractEdges`, we must also record the intention to check its
	// returned number of edges. This complicates the API and actually has never been
	// used so far. So we defer this enhancement for a later stage. Until then, the
	// recording API is only *mostly* compatible with the previous approach.
	_, err := w.RetractEdges(ctx, s.Node, s.Kind)
	if err != nil {
		return err
	}

	return nil
}

func (s retractEdges) Targets() iter.Seq[digitaltwin.Value] {
	return func(yield func(digitaltwin.Value) bool) {
		if !yield(s.Node) {
			return
		}
	}
}

// assertOneToOne is a Step that asserts a one-to-one relationship between two nodes.
type assertOneToOne struct {
	Source digitaltwin.Value
	Target digitaltwin.Value
}

func (s assertOneToOne) Do(ctx context.Context, w digitaltwin.GraphWriter) error {
	return assert.Graph(w).OneToOne(ctx, s.Source, s.Target)
}

func (s assertOneToOne) Targets() iter.Seq[digitaltwin.Value] {
	return func(yield func(digitaltwin.Value) bool) {
		if !yield(s.Source) {
			return
		}
		if !yield(s.Target) {
			return
		}
	}
}

// assertOneToMany is a Step that asserts a one-to-many relationship from source to target.
type assertOneToMany struct {
	Source digitaltwin.Value
	Target digitaltwin.Value
}

func (s assertOneToMany) Do(ctx context.Context, w digitaltwin.GraphWriter) error {
	return assert.Graph(w).OneToMany(ctx, s.Source, s.Target)
}

func (s assertOneToMany) Targets() iter.Seq[digitaltwin.Value] {
	return func(yield func(digitaltwin.Value) bool) {
		if !yield(s.Source) {
			return
		}
		if !yield(s.Target) {
			return
		}
	}
}

// assertManyToOne is a Step that asserts a many-to-one relationship from source to target.
type assertManyToOne struct {
	Source digitaltwin.Value
	Target digitaltwin.Value
}

func (s assertManyToOne) Do(ctx context.Context, w digitaltwin.GraphWriter) error {
	return assert.Graph(w).ManyToOne(ctx, s.Source, s.Target)
}

func (s assertManyToOne) Targets() iter.Seq[digitaltwin.Value] {
	return func(yield func(digitaltwin.Value) bool) {
		if !yield(s.Source) {
			return
		}
		if !yield(s.Target) {
			return
		}
	}
}

// assertManyToMany is a Step that asserts a many-to-many relationship between two nodes.
type assertManyToMany struct {
	Source digitaltwin.Value
	Target digitaltwin.Value
}

func (s assertManyToMany) Do(ctx context.Context, w digitaltwin.GraphWriter) error {
	return assert.Graph(w).ManyToMany(ctx, s.Source, s.Target)
}

func (s assertManyToMany) Targets() iter.Seq[digitaltwin.Value] {
	return func(yield func(digitaltwin.Value) bool) {
		if !yield(s.Source) {
			return
		}
		if !yield(s.Target) {
			return
		}
	}
}
