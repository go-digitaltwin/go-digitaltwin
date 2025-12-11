package assert_test

import (
	"context"
	"fmt"
	"reflect"

	"github.com/go-digitaltwin/go-digitaltwin"
	"github.com/go-digitaltwin/go-digitaltwin/assert"
)

// Read the doc of each relationship assertion for more details about its
// specific constraints.
func CommonRelationships(ctx context.Context, w digitaltwin.GraphWriter) error {
	fmt.Println("-- one to one --")
	_ = assert.Graph(w).OneToOne(ctx, Node{C: 'A'}, Node{C: 'B'})
	fmt.Println("-- one to many --")
	_ = assert.Graph(w).OneToMany(ctx, Node{C: 'C'}, Node{C: 'D'})
	fmt.Println("-- many to one --")
	_ = assert.Graph(w).ManyToOne(ctx, Node{C: 'E'}, Node{C: 'F'})
	fmt.Println("-- many to many --")
	_ = assert.Graph(w).ManyToMany(ctx, Node{C: 'G'}, Node{C: 'H'})
	return nil
}

// This example demonstrates how to use the assertions in this package in a way
// that reads well.
func Example() {
	// In this example, we ignore all errors.
	_ = printApplier{}.Apply(context.Background(), CommonRelationships)

	// Output:
	// -- one to one --
	// (A) <-/-> assert_test.Node
	// (B) <-/-> assert_test.Node
	// (A) -> (B)
	// -- one to many --
	// (D) <-/-> assert_test.Node
	// (C) -> (D)
	// -- many to one --
	// many nodes of type assert_test.Node may associate with (F)
	// -- many to many --
	// (G) -> (H)
}

// A Node represents an exemplar value in the graph for the examples in this
// package.
type Node struct {
	digitaltwin.InformationElement
	C rune
}

func (p Node) String() string {
	return fmt.Sprintf("(%c)", p.C)
}

// A printApplier applies compilations with a [digitaltwin.GraphWriter] that prints
// graph modifications to stdout. It also specialises specifically for [assert.ManyToOneAsserter] relationships.
type printApplier struct{}

func (x printApplier) Apply(ctx context.Context, compilation digitaltwin.Compilation) error {
	return compilation(ctx, x)
}

func (x printApplier) AssertNode(ctx context.Context, node digitaltwin.Value) (err error) {
	fmt.Println("+", node)
	return nil
}

func (x printApplier) RetractNode(ctx context.Context, node digitaltwin.Value) (err error) {
	fmt.Println("-", node)
	return nil
}

func (x printApplier) AssertEdge(ctx context.Context, from, to digitaltwin.Value) (err error) {
	fmt.Println(from, "->", to)
	return nil
}

func (x printApplier) RetractEdges(_ context.Context, node digitaltwin.Value, kind reflect.Type) (n int, err error) {
	fmt.Println(node, "<-/->", kind)
	return 0, nil
}

func (x printApplier) AssertManyToOne(ctx context.Context, source, target digitaltwin.Value) error {
	fmt.Printf("many nodes of type %T may associate with %v\n", source, target)
	return nil
}
