package digitaltwin

import (
	"context"
	"reflect"
)

// WhatChangeder defines an interface for implementing the observation of changes
// within a graph. Implementers of WhatChangeder are responsible for detecting
// and summarising changes in the graph since the last observation, with the goal
// of maintaining an accurate and up-to-date reflection of the digital twin's
// network.
//
// The WhatChanged method is the primary means of tracking these changes. It serves as an
// observer that methodically scans the graph for mutations, effectively differentiating
// between newly created, updated, and removed components. WhatChanged should provide
// high-level insights into the graph's evolution without the need for implementers to
// understand the intricacies of the underlying graph database mechanisms.
type WhatChangeder interface {
	WhatChanged(context.Context) (GraphChanged, error)
}

// A Compilation is a function that applies a set of mutations to a graph using
// the given GraphWriter and returns a non-nil error if those fail. It supports
// transactional semantics via an Applier.
//
// See the examples for demonstrations on how to write compilations.
type Compilation func(ctx context.Context, w GraphWriter) error

// An Applier applies a Compilation to a graph atomically and concurrently.
//
// It is up to the Applier to maintain the graph's data integrity; therefore, any
// Compilations that fail must not commit changes to the graph.
//
// An Applier's Apply method is called concurrently. Thus, implementations must
// allow for concurrent execution.
//
// A Compilation is called with a GraphWriter. It's up to the implementation to
// determine how to acquire such a GraphWriter and pass it to each Compilation.
type Applier interface {
	Apply(ctx context.Context, compilation Compilation) error
}

// GraphWriter defines the operations digital-twins may use to modify their
// graphs. Specific graph engines (e.g. Neo4j) are expected to implement these
// primitive operations.
type GraphWriter interface {
	// AssertNode guarantees that by the time is returns with a nil error, the
	// provided [Value] will have been present as a node in the digital-twin's graph.
	//
	// If the Value is already present in the graph, the function has no meaningful
	// effect (though implementations may update metadata about the node
	// corresponding to that value in the graph engine), and a nil error is returned.
	// Otherwise, it attempts to insert a new node with properties that correspond to
	// the provided value.
	AssertNode(ctx context.Context, node Value) (err error)

	// RetractNode guarantees that by the time is returns with a nil error, the
	// provided [Value] will have no longer been represented as a node in the
	// digital-twin's graph.
	//
	// If the Value is not present in the graph, the function has no meaningful
	// effect and a nil error is returned. Otherwise, it attempts to remove the node
	// that corresponds to the provided value.
	RetractNode(ctx context.Context, node Value) (err error)

	// AssertEdge guarantees that by the time it returns with a nil error, a directed
	// edge from the source [Value] node to the destination [Value] node will have
	// been present in the digital-twin's graph. It also ensures that both the source
	// and destination nodes exist in the graph, creating them if they do not.
	//
	// If the edge already exists, the function has no meaningful effect (though
	// implementations may update metadata about the edge within the graph engine),
	// and a nil error is returned. If one or both nodes do not exist, the function
	// will create them and then attempt to create the new edge that connects the
	// `from` node to the `to` node.
	AssertEdge(ctx context.Context, from, to Value) (err error)

	// RetractEdges guarantees that by the time it returns with a nil error, any
	// edges from the given node to other nodes of the given kind will have been
	// removed from the digital-twin's graph.
	//
	// If no such edges are present, the function has no meaningful effect and a nil
	// error is returned. Otherwise, it attempts to remove any edges that satisfy the
	// criteria of having the given node at one end and another node of the given
	// kind at the other, regardless of the edge's direction. Either way, the number
	// of detached relationships (i.e. removed edges) is returned.
	//
	// The exact graph node is uniquely identified by the content-address of the
	// given Value.
	RetractEdges(ctx context.Context, node Value, kind reflect.Type) (n int, err error)
}
