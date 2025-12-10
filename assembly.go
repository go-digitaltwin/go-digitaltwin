package digitaltwin

import (
	"fmt"
	"strings"
	"time"
)

// Assembly is a self-sufficient data structure that contains a set of properties
// on nodes in a directed graph.
//
// Do not modify the values returned from its functions.
type Assembly interface {
	AssemblyRef
	Roots() []NodeHash
	Nodes() map[NodeHash]Value
	Value(n NodeHash) Value
	EdgesOf(n NodeHash) []NodeHash
	VisitEdges(fn func(from, to Value) bool)
}

// AssemblyRef exposes methods to consistently reference component-graphs across
// our distributed system. A consistent reference consists of a unique computed
// identifier (i.e., AssemblyID) and a hash of the graph (i.e., AssemblyHash).
type AssemblyRef interface {
	// AssemblyID computes an identifying hash over the root nodes of the component.
	// The root of a directed graph is defined as the subset of its nodes without any
	// ingres (i.e., with only egress) edges.
	AssemblyID() ComponentID
	// AssemblyHash computes a content hash over the entire graph (i.e., edges and
	// nodes with attached attributes).
	AssemblyHash() ComponentHash
}

// ComputeForestHash digests the given components into a ForestHash.
//
// The functions AssemblyRef.AssemblyID() and AssemblyRef.AssemblyHash() are
// being cached internally, hence they must return a consistent value during the
// lifetime of this computation.
func ComputeForestHash(components ...AssemblyRef) ForestHash {
	// pre-compute all component references (id and hash) to preserve
	// redundant computation during sorting
	precomputed := make(map[ComponentID]ComponentHash, len(components))
	for i := range components {
		precomputed[components[i].AssemblyID()] = components[i].AssemblyHash()
	}
	return HashComponents(precomputed)
}

// GraphChanged notifies the internal graph-based world-view maintained by a
// digital-twin has changed. The message contains the bulk changeset relative to
// the previously notified baseline. This baseline state of the graph is hashed
// as GraphBefore. The latest state of the graph at the time of this
// notification is hashed as GraphAfter.
type GraphChanged struct {
	GraphBefore ForestHash
	Created     []AssemblyCreated
	Updated     []AssemblyUpdated
	Removed     []AssemblyRemoved
	GraphAfter  ForestHash
	// The time, in UTC, the graph change was computed. The information in this
	// message is accurate up to this timestamp, not a moment afterwards.
	Timestamp time.Time
}

// IsEmpty returns true if the notification contains no changes. Meaning, the
// graph had not changed between GraphBefore and GraphAfter.
func (c GraphChanged) IsEmpty() bool {
	return c.GraphAfter == c.GraphBefore
}

// AssemblyCreated notifies about a new graph component that has been added to
// the complete graph maintained by a digital twin.
//
// The message contains both the new component graph and its reference, i.e.,
// exposing AssemblyRef.AssemblyID() and AssemblyRef.AssemblyHash().
type AssemblyCreated struct {
	Assembly // an independent representation of the component graph
}

// AssemblyUpdated notifies about a modification to an existing (through a
// concomitant AssemblyCreated notification) component.
//
// The message contains both the modified component graph (like AssemblyCreated)
// and a Baseline hash referencing the latest snapshot of the component graph
// before is has been updated.
type AssemblyUpdated struct {
	Baseline ComponentHash // content address of the component graph before the update
	Assembly               // an independent representation of the component graph
}

// AssemblyRemoved notifies about the disappearance of an existing (through a
// concomitant AssemblyCreated or AssemblyUpdated notification) component.
//
// The message contains both the ID of the removed component graph and a Hash
// referencing the latest snapshot of the component graph before it has been
// removed.
//
// Although an AssemblyRemoved represents an assembly that no longer exists, it
// still implements the Assembly interface. It follows an 'empty' pattern
// implementation since a removed assembly is still considered an assembly,
// albeit an empty one.
type AssemblyRemoved struct {
	ID   ComponentID   // identifier of the removed component
	Hash ComponentHash // hash of the removed component's graph
}

func (c AssemblyRemoved) AssemblyID() ComponentID     { return c.ID }
func (c AssemblyRemoved) AssemblyHash() ComponentHash { return c.Hash }

// Roots returns a nil slice because an empty assembly has no roots.
func (c AssemblyRemoved) Roots() []NodeHash {
	return nil
}

// Nodes returns a nil map because an empty assembly has no nodes.
func (c AssemblyRemoved) Nodes() map[NodeHash]Value {
	return nil
}

// Value returns nil, which is the zero value of the Value interface, because an
// empty assembly has no nodes.
func (c AssemblyRemoved) Value(NodeHash) Value {
	return nil
}

// EdgesOf returns a nil slice because an empty assembly has no nodes.
func (c AssemblyRemoved) EdgesOf(NodeHash) []NodeHash {
	return nil
}

// VisitEdges returns without performing any operations because an empty assembly
// has no nodes, and consequently, no edges to visit.
func (c AssemblyRemoved) VisitEdges(func(from, to Value) bool) {}

// FormatChanges returns a human-readable representation of the changeset.
// The indent string is prepended to each line.
func FormatChanges(changes GraphChanged, indent string) string {
	var b strings.Builder
	fmt.Fprintf(&b, indent+"baseline snapshot: %v\n", changes.GraphBefore)
	for _, c := range changes.Created {
		fmt.Fprintf(&b, indent+"+ %v | %v)\n", c.AssemblyID(), c.AssemblyHash())
		c.VisitEdges(func(s, t Value) bool {
			fmt.Fprintf(&b, indent+"  %v -> %v\n", s, t)
			return true
		})
	}
	for _, c := range changes.Updated {
		fmt.Fprintf(&b, indent+"* %v | %v\n", c.AssemblyID(), c.AssemblyHash())
		c.VisitEdges(func(s, t Value) bool {
			fmt.Fprintf(&b, indent+"  %v -> %v\n", s, t)
			return true
		})
	}
	for _, c := range changes.Removed {
		fmt.Fprintf(&b, indent+"- %v | %v\n", c.AssemblyID(), c.AssemblyHash())
	}
	fmt.Fprintf(&b, indent+"current snapshot: %v\n", changes.GraphAfter)
	return b.String()
}
