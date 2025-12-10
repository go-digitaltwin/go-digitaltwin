package digitaltwin

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
