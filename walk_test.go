package digitaltwin

import (
	"fmt"
	"slices"
	"testing"
)

func TestInspect(t *testing.T) {
	// Create the assembly for the test.
	//     ┌─ DDD
	//     │
	//   BB┤
	//   │ │
	//   │ └─ EEE
	//   │
	//A──┤
	//   │
	//   │ ┌─ FFF
	//   │ │
	//   CC┤
	//     │
	//     └─ GGG

	var builder AssemblyBuilder
	// Root
	builder.Roots(fakeNode{Value: "A"})
	// First level children
	builder.Connect(fakeNode{Value: "A"}, fakeNode{Value: "BB"})
	builder.Connect(fakeNode{Value: "A"}, fakeNode{Value: "CC"})
	//Second level children
	builder.Connect(fakeNode{Value: "BB"}, fakeNode{Value: "DDD"})
	builder.Connect(fakeNode{Value: "BB"}, fakeNode{Value: "EEE"})
	builder.Connect(fakeNode{Value: "CC"}, fakeNode{Value: "FFF"})
	builder.Connect(fakeNode{Value: "CC"}, fakeNode{Value: "GGG"})

	visited := make(map[fakeNode]struct{})
	var visitOrder []fakeNode

	testFunc := func(value Value) bool {
		// Must check if value is nil before type assertion
		if value == nil {
			return false
		}
		v := value.(fakeNode)
		visited[v] = struct{}{}
		visitOrder = append(visitOrder, v)
		return true
	}

	assembly := builder.Assemble()
	Inspect(assembly, testFunc)

	for _, value := range assembly.Nodes() {
		v := value.(fakeNode)
		if _, seen := visited[v]; !seen {
			t.Errorf("Inspect did not visit all nodes: %q wasn't visited", v.Value)
		}
	}

	root := assembly.Value(assembly.Roots()[0]).(fakeNode)
	for node := range visited {
		// The root node is irrelevant because
		if node == root {
			continue
		}

		ca := MustContentAddress(node)
		children := assembly.EdgesOf(ca)
		for _, child := range children {
			// Although slices.Index returns -1 when if cannot find the node, but the
			// previous test already checks that all the nodes in the graph were visited.
			childPos := slices.Index(visitOrder, assembly.Value(child).(fakeNode))
			nodePos := slices.Index(visitOrder, node)
			if childPos < nodePos {
				t.Errorf("Node %v (at %d) was visited before its parent %v (at %d)", child, childPos, node, nodePos)
			}
		}
	}
}

func ExampleInspect() {
	var builder AssemblyBuilder

	builder.Roots(fakeNode{Value: "A"})
	builder.Connect(fakeNode{Value: "A"}, fakeNode{Value: "AB"})
	builder.Connect(fakeNode{Value: "AB"}, fakeNode{Value: "ABC"})

	printFunc := func(value Value) bool {
		fmt.Println(value)
		return true
	}

	Inspect(builder.Assemble(), printFunc)
	// Output:
	// A
	// AB
	// ABC
	// <nil>
	// <nil>
	// <nil>
}

type fakeNode struct {
	InformationElement
	Value string
}

func (f fakeNode) String() string {
	return f.Value
}
