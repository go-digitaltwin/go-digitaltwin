package digitaltwin

import (
	"bytes"
	"crypto/sha1"
	"encoding/gob"
	"sort"
	"unsafe"
)

// An AssemblyBuilder is used to safely and elegantly build an Assembly (graph component)
// using fluent calls.
// The zero value is ready to use.
// Do not copy a non-zero AssemblyBuilder.
type AssemblyBuilder struct {
	roots      []NodeHash
	nodes      map[NodeHash]Value
	neighbours map[NodeHash]map[NodeHash]struct{}
	// address of receiver - to detect copies by value.
	// see copyCheck below for details.
	addr *AssemblyBuilder
}

// Assemble returns the accumulated Assembly.
func (b *AssemblyBuilder) Assemble() Assembly {
	var g AssemblyGraph

	// copy roots slice to allow further modifications to the builder
	if len(b.roots) != 0 {
		g.Root = make([]NodeHash, len(b.roots))
		copy(g.Root, b.roots)
	}

	// copy nodes map to allow further modifications to the builder
	if len(b.nodes) != 0 {
		g.Vertices = make(map[NodeHash]Value, len(b.nodes))
		for id, n := range b.nodes {
			g.Vertices[id] = n
		}
	}

	// copy edges map to allow further modifications to the builder
	if len(b.neighbours) != 0 {
		g.Neighbours = make(map[NodeHash][]NodeHash, len(b.neighbours))
		for id, neighbours := range b.neighbours {
			g.Neighbours[id] = make([]NodeHash, 0, len(neighbours))
			for n := range neighbours {
				g.Neighbours[id] = append(g.Neighbours[id], n)
			}
		}
	}

	return g
}

// Reset resets the Builder to be empty.
func (b *AssemblyBuilder) Reset() {
	b.roots = nil
	b.nodes = nil
	b.neighbours = nil
	b.addr = nil
}

// Nodes shall append the given nodes to b's node list.
func (b *AssemblyBuilder) Nodes(node ...Value) {
	b.copyCheck()
	if b.nodes == nil {
		b.nodes = make(map[NodeHash]Value, len(node))
	}
	for _, n := range node {
		id := MustContentAddress(n)
		b.nodes[id] = n
	}
}

// Connect shall append the given from and to nodes to b's node list and
// a directed edge between them.
func (b *AssemblyBuilder) Connect(source, target Value) {
	b.copyCheck()
	b.Nodes(source, target)
	from := MustContentAddress(source)
	to := MustContentAddress(target)
	if b.neighbours == nil {
		b.neighbours = make(map[NodeHash]map[NodeHash]struct{})
	}
	if b.neighbours[from] == nil {
		b.neighbours[from] = make(map[NodeHash]struct{})
	}
	b.neighbours[from][to] = struct{}{}
}

// Roots shall replace b's existing root nodes list with the given roots.
func (b *AssemblyBuilder) Roots(root ...Value) {
	b.copyCheck()
	b.Nodes(root...)
	if len(b.roots) > len(root) { // trim down if there is b.roots is long enough
		b.roots = b.roots[:len(root)]
	} else { // prepare enough capacity for the new root-nodes
		b.roots = make([]NodeHash, len(root))
	}
	for i, n := range root {
		b.roots[i] = MustContentAddress(n)
	}
}

// hintNodes copies the internal nodes-map to a new, larger map so that there
// are approximately (depends on map implementation) n nodes of capacity to
// b.nodes without requiring rehashing and allocations.
func (b *AssemblyBuilder) hintNodes(n int) {
	// the calculation (i.e., len = 2*len + extra) is based on strings/builder.go (Builder.Grow)
	nodes := make(map[NodeHash]Value, 2*len(b.nodes)+n)
	for k, v := range b.nodes {
		nodes[k] = v
	}
	b.nodes = nodes
}

/*
// hintEdges copies the internal edges-map to a new, larger map so that there
// are approximately (depends on map implementation) e edges of capacity to
// b.edges without requiring rehashing and allocations.
func (b *AssemblyBuilder) hintEdges(e int) {
	// the calculation (i.e., len = 2*len + extra) is based on strings/builder.go (Builder.Grow)
	neighbours := make(map[NodeHash]map[NodeHash]struct{}, 2*len(b.neighbours)+e)
	for from, tos := range b.neighbours {
		neighbours[from] = make(map[NodeHash]struct{}, len(b.nodes))
		neighbours[k] = v
	}
	b.neighbours = neighbours
}
*/

// Hint hints b's map size, if necessary, to guarantee space for more n nodes
// and e edges. After Grow(n, e), approximately (depends on map implementation)
// n nodes and e edges can be added to b without another allocation.
// If either n or e is negative, Grow shall panic.
func (b *AssemblyBuilder) Hint(n, e int) {
	b.copyCheck()

	if n < 0 {
		panic("digitaltwin.AssemblyBuilder.Grow: negative node count")
	}
	if len(b.nodes) < n {
		b.hintNodes(n)
	}

	if e < 0 {
		panic("digitaltwin.AssemblyBuilder.Grow: negative edge count")
	}
	//if len(b.neighbours) < e {
	//	b.hintEdges(e)
	//}
}

// Noescape hides a pointer from escape analysis.
// It is the identity function, but escape analysis does not think the
// output depends on the input.
// Noescape is inlined and currently compiles down to zero instructions.
// USE CAREFULLY!
// This was copied from the runtime; see issues 23382 and 7921 (github.com/golang/go).
//
//go:nosplit
//go:nocheckptr
func noescape(p unsafe.Pointer) unsafe.Pointer {
	x := uintptr(p)
	return unsafe.Pointer(x ^ 0) //nolint:govet,staticcheck,gosec // copied from the standard library
}

func (b *AssemblyBuilder) copyCheck() {
	if b.addr == nil {
		// This hack works around a failing of Go's escape analysis
		// that was causing b to escape and be heap-allocated.
		// See issue 23382 (github.com/golang/go).
		// once issue 7921 is fixed, this should be reverted to just "b.addr = b".
		b.addr = (*AssemblyBuilder)(noescape(unsafe.Pointer(b)))
	} else if b.addr != b {
		panic("digitaltwin: illegal use of non-zero AssemblyBuilder copied by value")
	}
}

//=============================================================================

func init() {
	gob.Register(AssemblyGraph{})
}

// AssemblyGraph is a directed graph-based representation of an Assembly.
// DO NOT modify its Roots, Vertices and Neighbours manually.
type AssemblyGraph struct {
	Root       []NodeHash
	Vertices   map[NodeHash]Value
	Neighbours map[NodeHash][]NodeHash
}

func (a AssemblyGraph) Roots() []NodeHash             { return a.Root }
func (a AssemblyGraph) Nodes() map[NodeHash]Value     { return a.Vertices }
func (a AssemblyGraph) Value(n NodeHash) Value        { return a.Vertices[n] }
func (a AssemblyGraph) EdgesOf(n NodeHash) []NodeHash { return a.Neighbours[n] }

func (a AssemblyGraph) VisitEdges(fn func(from, to Value) bool) {
	for from, neighbours := range a.Neighbours {
		for _, to := range neighbours {
			if !fn(a.Vertices[from], a.Vertices[to]) {
				return
			}
		}
	}
}

func (a AssemblyGraph) AssemblyID() ComponentID {
	h := sha1.New()
	// sort lexicographically to achieve consistency
	sort.Slice(a.Root, func(i, j int) bool {
		return bytes.Compare(a.Root[i][:], a.Root[j][:]) < 0
	})
	// hash root nodes in sorted order
	for i := range a.Root {
		h.Write(a.Root[i][:])
	}
	return ComponentID(h.Sum(nil))
}

func (a AssemblyGraph) AssemblyHash() ComponentHash {
	h := sha1.New()
	// don't forget to hash the ID (roots)
	id := a.AssemblyID()
	h.Write(id[:])

	// sort nodes lexicographically
	nodes := make([]NodeHash, 0, len(a.Vertices))
	for n := range a.Vertices {
		nodes = append(nodes, n)
	}
	sort.Slice(nodes, func(i, j int) bool {
		return bytes.Compare(nodes[i][:], nodes[j][:]) < 0
	})

	// hash nodes in sorted order, then hash their sorted neighbours
	for _, from := range nodes {
		h.Write(from[:])
		neighbours := a.Neighbours[from]
		sort.Slice(neighbours, func(i, j int) bool {
			return bytes.Compare(neighbours[i][:], neighbours[j][:]) < 0
		})
		for _, to := range neighbours {
			h.Write(to[:])
		}
	}
	return ComponentHash(h.Sum(nil))
}
