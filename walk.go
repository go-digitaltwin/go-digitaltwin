package digitaltwin

// A Visitor defines a Visit method invoked for each Value (i.e. a node in a
// disjoint graph component) encountered by Walk. If the result visitor w is not
// nil, Walk visits each child of the node with the visitor w, followed by a call
// of w.Visit(nil).
type Visitor interface {
	Visit(node Value) (w Visitor)
}

// Walk traverses an Assembly in depth-first order: It calls WalkSubtree(root)
// for each of the root nodes in the given disjoint graph-component; the tree
// must not be nil.
func Walk(v Visitor, tree Assembly) {
	for _, root := range tree.Roots() {
		WalkSubtree(v, tree, root)
	}
}

// WalkSubtree traverses a subtree within an Assembly in depth-first order: It
// starts by calling v.Visit(node) with the Value of the given node. If the
// visitor w returned by v.Visit(node) is not nil, walk is invoked recursively
// with visitor w for each child of the node, followed by a call of w.Visit(nil).
func WalkSubtree(v Visitor, tree Assembly, node NodeHash) {
	// Start by calling v.Visit(node).
	if v = v.Visit(tree.Value(node)); v == nil {
		return
	}
	// Then traverse the tree of the given node, depth-first.
	for _, child := range tree.EdgesOf(node) {
		WalkSubtree(v, tree, child)
	}
	// Finally, call v.Visit(nil).
	v.Visit(nil)
}

type inspector func(value Value) bool

func (f inspector) Visit(node Value) Visitor {
	if f(node) {
		return f
	}
	return nil
}

// Inspect traverses an Assembly in depth-first order: It starts by calling
// f(root) for every root of the given tree; the tree must not be nil. If f
// returns true, Inspect invokes f recursively for each child of the root node,
// followed by a call of f(nil).
func Inspect(tree Assembly, f func(value Value) bool) {
	Walk(inspector(f), tree)
}
