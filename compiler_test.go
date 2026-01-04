package digitaltwin_test

import (
	"context"
	"fmt"
	"reflect"

	"github.com/go-digitaltwin/go-digitaltwin"
)

// It is common for a Compilation to be an anonymous function that captures in
// its closure the [Value] nodes with which the graph is modified.
func ExampleCompilation_anonymous() {
	var applier printApplier

	// Usually, a specific digital-twin defines a set of "compiler" functions. Each
	// compiler returns a compilation that modifies the graph according to its
	// domain-specific parameters.
	compiler := func(from, to dummyNode) digitaltwin.Compilation {
		// A compilation is often an anonymous function because its signature forces that
		// the values (to be represented in the graph) are passed in via closure.
		return func(ctx context.Context, w digitaltwin.GraphWriter) error {
			// Interesting digital-twins make interesting modifications to their graphs.
			// However, in this example we trivially desire a single edge between a pair of
			// nodes.
			return w.AssertEdge(ctx, from, to)
		}
	}

	_ = applier.Apply(context.Background(), compiler(dummyNode{Value: "A"}, dummyNode{Value: "B"}))
	// Output:
	// (A) -> (B)
}

// This example showcases the relationship between an [digitaltwin.Applier] and
// [digitaltwin.GraphWriter] interfaces.
func ExampleApplier() {
	// Specific graph engines (e.g. neo4j) provide types that implement the Applier
	// interface. This example relies on the printApplier.
	applier := printApplier{}

	_ = applier.Apply(context.Background(), func(ctx context.Context, w digitaltwin.GraphWriter) error {
		_ = w.AssertNode(ctx, dummyNode{Value: "A"})
		_ = w.RetractNode(ctx, dummyNode{Value: "A"})
		_ = w.AssertEdge(ctx, dummyNode{Value: "A"}, dummyNode{Value: "B"})
		_, _ = w.RetractEdges(ctx, dummyNode{Value: "A"}, reflect.TypeOf(dummyNode{}))
		return nil
	})
	// Output:
	// + (A)
	// - (A)
	// (A) -> (B)
	// (A) <-/-> digitaltwin_test.dummyNode
}

// A dummyNode demonstrates a node in the graph for the examples in this package.
// Each digital-twin domain has its own complex types of values.
type dummyNode struct {
	digitaltwin.InformationElement
	Value string
}

func (p dummyNode) String() string {
	return fmt.Sprintf("(%s)", p.Value)
}

// A printApplier applies compilations with a digitaltwin.GraphWriter that prints
// graph modifications to stdout.
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
