/*
Package compilation enables distributed compilations across processes by
enabling callers to create reproducible graph mutations that can be stored,
transmitted, and applied consistently across different environments.

The package provides a [Recorder] for collecting and managing graph mutation
steps, and a [Replay] function for executing these steps. This enables efficient
recording, storage, and replay of graph operations, supporting distributed
processing and transaction-like behaviour in digital twin graphs.

The compilation package decouples between domain-specific operations and
applying graph mutations, providing a clear separation of responsibilities
between components and digital twin graphs.
*/
package compilation

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"iter"
	"reflect"

	"github.com/go-digitaltwin/go-digitaltwin"
)

// Step represents a single atomic mutation operation on a digital twin's graph.
// Each Step encapsulates a specific change that can be applied to the graph.
//
// In distributed compilation scenarios, Steps form the fundamental units of work
// that can be serialised and transmitted across process boundaries.
//
// All Step implementations must be properly registered with gob to ensure
// consistent behaviour across environments.
type Step interface {
	// Do applies the mutation to the graph using the provided
	// digitaltwin.GraphWriter. It transforms the graph according to the Step's
	// specific semantics and returns an error if the mutation cannot be applied due
	// to constraints or inconsistencies.
	Do(context.Context, digitaltwin.GraphWriter) error
	// Targets return a sequence of nodes that this Step affects. This is used to
	// identify which nodes are involved in the mutation, allowing engines to track
	// dependencies between steps and optimise execution.
	Targets() iter.Seq[digitaltwin.Value]
}

// Encode serialises a slice of Steps into a byte array for storage or
// transmission. It transforms compilation steps into a portable format that can
// cross process boundaries while preserving their semantic meaning.
//
// The function uses gob encoding to ensure consistent serialisation across Go
// environments. It returns the encoded bytes and any error encountered during
// the encoding process.
func Encode(s []Step) (data []byte, err error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	if err := encoder.Encode(s); err != nil {
		return nil, fmt.Errorf("gob encode: %w", err)
	}
	return buf.Bytes(), nil
}

// Decode reconstructs a slice of Steps from a previously encoded byte array. It
// restores compilation steps from their portable format back into executable
// graph mutations that can be replayed in any compatible environment.
//
// This function is essential for distributed compilations, enabling steps
// recorded in one process to be faithfully reproduced in another. It returns the
// decoded steps and any error encountered during the decoding process.
func Decode(data []byte) (steps []Step, err error) {
	var s []Step
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&s); err != nil {
		return nil, fmt.Errorf("gob decode: %w", err)
	}
	return s, nil
}

// Recorder collects a sequence of graph mutations (assertions and retractions)
// that can be applied to a graph via a [digitaltwin.GraphWriter]. Each mutation
// is stored as a separate [Step] in the order it was added.
//
// The Recorder acts as the primary entry point for building compilations,
// providing methods to express graph transformations in domain terms rather than
// low-level graph operations.
//
// The zero value of Recorder is ready to use. Do not copy a non-zero Recorder.
type Recorder struct {
	steps []Step
}

// Reset clears all accumulated steps, returning the Recorder to its initial
// empty state. This allows the Recorder to be reused for a new compilation
// sequence without allocating a new instance.
func (r *Recorder) Reset() {
	r.steps = nil
}

// Steps returns a copy of the currently recorded mutation steps. The returned
// slice represents the complete sequence of graph transformations captured by
// this Recorder.
//
// Modifying the returned slice does not affect the Recorder's internal state,
// ensuring the integrity of the original recording.
func (r *Recorder) Steps() []Step {
	s := make([]Step, len(r.steps))
	copy(s, r.steps)
	return s
}

// Replay creates a [digitaltwin.Compilation] function that sequentially applies
// a series of mutation steps. It transforms recorded steps into an executable
// compilation that can be applied to a graph.
//
// The returned compilation will process each [Step] in order using the provided
// [digitaltwin.GraphWriter], applying the recorded mutations to the graph in a
// consistent and reproducible manner.
//
// If any step fails during execution, the process stops immediately and returns
// the error, leaving the graph in a partially modified state. Callers may need
// to implement additional error handling or transaction-like behaviour if
// atomicity is required.
func Replay(steps []Step) digitaltwin.Compilation {
	return func(ctx context.Context, w digitaltwin.GraphWriter) error {
		for _, step := range steps {
			if err := step.Do(ctx, w); err != nil {
				return err
			}
		}
		return nil
	}
}

// Targets iterate over all nodes affected by the provided steps, yielding each
// target node to the provided function once.
//
// This function is useful for identifying which nodes are involved in the
// compilation steps, allowing callers to understand the scope of the mutations
// being applied.
func Targets(steps []Step) iter.Seq[digitaltwin.Value] {
	return func(yield func(digitaltwin.Value) bool) {
		// We are using [digitaltwin.NodeHash] because for each [digitaltwin.Value] there
		// is a 1-to-1 mapping to a [digitaltwin.NodeHash].
		var seen = make(map[digitaltwin.NodeHash]struct{})
		for _, step := range steps {
			for target := range step.Targets() {
				// Ensure we only yield each target node once.
				ca := digitaltwin.MustContentAddress(target)
				if _, ok := seen[ca]; ok {
					continue
				}
				seen[ca] = struct{}{}
				if !yield(target) {
					return
				}
			}
		}
	}
}

// AssertNode records a mutation step that will assert a node in the graph.
//
// When replayed, this step ensures the specified node exists in the graph,
// creating it if it doesn't already exist. The node's identity is preserved
// throughout the compilation process.
func (r *Recorder) AssertNode(node digitaltwin.Value) {
	r.steps = append(r.steps, assertNode{Node: node})
}

// RetractNode records a mutation step that will retract a node from the graph.
//
// When replayed, this step removes the specified node from the graph along with
// all its connected edges. This is a destructive operation that removes both the
// node and all its relationships in a single step.
func (r *Recorder) RetractNode(node digitaltwin.Value) {
	r.steps = append(r.steps, retractNode{Node: node})
}

// AssertEdge records a mutation step that will assert an edge between two nodes.
//
// When replayed, this step establishes a directed relationship between the
// specified source and target nodes. It also ensures that both the source and
// target nodes exist in the graph, creating them if they do not.
//
// The edge direction is significant and preserved during compilation.
func (r *Recorder) AssertEdge(from, to digitaltwin.Value) {
	r.steps = append(r.steps, assertEdge{From: from, To: to})
}

// RetractEdges records a mutation step that will retract edges from a node.
//
// When replayed, this step removes all edges from the specified node to nodes of
// the specified type. This is a bulk operation that affects all matching
// outgoing relationships in a single step.
func (r *Recorder) RetractEdges(node digitaltwin.Value, kind reflect.Type) {
	r.steps = append(r.steps, retractEdges{Node: node, Kind: kind})
}

// AssertOneToOne records a mutation step that will assert a one-to-one
// relationship between the source and target nodes.
//
// When replayed, this step ensures that the source node has exactly one outgoing
// edge to the target node and the target node has exactly one incoming edge from
// the source node, representing this specific relationship type. Other
// relationships are not affected. If the relationship already exists, this
// operation is a no-op. If either node does not exist, it will be created.
func (r *Recorder) AssertOneToOne(source, target digitaltwin.Value) {
	r.steps = append(r.steps, assertOneToOne{Source: source, Target: target})
}

// AssertOneToMany records a mutation step that will assert a one-to-many
// relationship from the source node to the target node.
//
// When replayed, this step ensures that the source node is connected to the
// target node, allowing the source to have multiple such connections to
// different target nodes, but each target node is connected to at most one
// source node in this specific relationship type. If either node does not exist,
// it will be created.
func (r *Recorder) AssertOneToMany(source, target digitaltwin.Value) {
	r.steps = append(r.steps, assertOneToMany{Source: source, Target: target})
}

// AssertManyToOne records a mutation step that will assert a many-to-one
// relationship from the source node to the target node.
//
// When replayed, this step ensures that the source node is connected to the
// target node. This allows multiple source nodes to connect to the same target
// node, but each source node can only connect to one target node in this
// specific relationship type. If either node does not exist, it will be created.
func (r *Recorder) AssertManyToOne(source, target digitaltwin.Value) {
	r.steps = append(r.steps, assertManyToOne{Source: source, Target: target})
}

// AssertManyToMany records a mutation step that will assert a many-to-many
// relationship between the source and target nodes.
//
// When replayed, this step ensures that the source node is connected to the
// target node. This allows multiple source nodes to connect to multiple target
// nodes and vice-versa. If either node does not exist, it will be created.
func (r *Recorder) AssertManyToMany(source, target digitaltwin.Value) {
	r.steps = append(r.steps, assertManyToMany{Source: source, Target: target})
}
