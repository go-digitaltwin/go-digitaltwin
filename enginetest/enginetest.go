/*
Package enginetest provides a suite of tests designed to assess digital-twin
graph engines (e.g. in-memory, neo4j).

The tests operate on the specific graph engine via the [digitaltwin.Applier] and
[digitaltwin.WhatChangeder] interfaces to check functional correctness and
compliance with the behaviours defined by those interfaces.

Call enginetest.Run in its own test to invoke the test-suite:

	func TestEngine(t *testing.T) {
		graph := graphlib.New() // Create a new underlying graph engine.
		// Create a new digital-twin engine based on the underlying graph model.
		engine := NewEngine(context.Background(), graph)
		// Call enginetest.Run, passing the digital-twin engine as both
		// digitaltwin.Applier and digitaltwin.WhatChangeder implementation.
		enginetest.Run(t, engine, engine)
	}

The test cases in this suite focus on the basic graph operations:

  - Modifying disjoint graph components by connecting and disconnecting nodes.
  - Observing changes to disjoint graph components over time.

So, specific digital-twin graph engines are encouraged to perform additional
tests which are specific to the underlying graph engine.
*/
package enginetest

import (
	"context"
	"encoding/gob"
	"fmt"
	"reflect"
	"runtime"
	"testing"

	"github.com/go-digitaltwin/go-digitaltwin"
	"github.com/go-digitaltwin/go-digitaltwin/assert"
)

func init() {
	gob.Register(NodeA{})
	gob.Register(NodeB{})
	gob.Register(NodeC{})
	gob.Register(NodeD{})
}

type NodeA struct{ digitaltwin.InformationElement }
type NodeB struct{ digitaltwin.InformationElement }
type NodeC struct{ digitaltwin.InformationElement }
type NodeD struct{ digitaltwin.InformationElement }

type testCase struct {
	// Subtest name.
	name string
	// A path leading to the test-case's file and line in the source code.
	location string
	// A compilation executes a single modification on the tested digital-twin's
	// graph using its argument [digitaltwin.GraphWriter].
	compilation digitaltwin.Compilation
	// A list of checks to run on the resulting [digitaltwin.GraphChanged]. Keep in
	// mind failing to specify any of created, updated, or removed causes the
	// test-case to not verify the respective field in the case's
	// [digitaltwin.GraphChanged] message.
	checks []check
	// A snapshot of the entire graph as expected after the compilation has been
	// applied successfully. This snapshot takes into account the order and the
	// successful execution of previous test-cases.
	graph snapshot
}

var cases = []testCase{
	{
		name:     "retract-nonexistent-node",
		location: locateSource(),
		compilation: func(ctx context.Context, w digitaltwin.GraphWriter) error {
			return w.RetractNode(ctx, NodeA{})
		},
		graph: snapshot{},
		checks: []check{
			created(),
			updated(),
			removed(),
		},
	},
	{
		name:     "retract-nonexistent-edge",
		location: locateSource(),
		compilation: func(ctx context.Context, w digitaltwin.GraphWriter) error {
			n, err := w.RetractEdges(ctx, NodeA{}, reflect.TypeOf(NodeB{}))
			if err != nil {
				return err
			}
			if n != 0 {
				return fmt.Errorf("expected zero edges, got %d", n)
			}
			return nil
		},
		graph: snapshot{},
		checks: []check{
			created(),
			updated(),
			removed(),
		},
	},
	{
		name:     "new-node",
		location: locateSource(),
		compilation: func(ctx context.Context, w digitaltwin.GraphWriter) error {
			return w.AssertNode(ctx, NodeA{})
		},
		graph: snapshot{tree(NodeA{})},
		checks: []check{
			created(tree(NodeA{})),
			updated(),
			removed(),
		},
	},
	{
		name:     "delete-node",
		location: locateSource(),
		compilation: func(ctx context.Context, w digitaltwin.GraphWriter) error {
			return w.RetractNode(ctx, NodeA{})
		},
		graph: snapshot{},
		checks: []check{
			created(),
			updated(),
			removed(tree(NodeA{})),
		},
	},
	{
		name:     "connect-tree",
		location: locateSource(),
		compilation: func(ctx context.Context, w digitaltwin.GraphWriter) error {
			return assert.Graph(w).OneToOne(ctx, NodeA{}, NodeB{})
		},
		graph: snapshot{tree(NodeA{}, NodeB{})},
		checks: []check{
			created(tree(NodeA{}, NodeB{})),
			updated(),
			removed(),
		},
	},
	{
		name:     "extend-tree",
		location: locateSource(),
		compilation: func(ctx context.Context, w digitaltwin.GraphWriter) error {
			return assert.Graph(w).OneToOne(ctx, NodeB{}, NodeC{})
		},
		graph: snapshot{tree(NodeA{}, NodeB{}, NodeC{})},
		checks: []check{
			created(),
			updated(tree(NodeA{}, NodeB{}, NodeC{})),
			removed(),
		},
	},
	{
		name:     "split-tree",
		location: locateSource(),
		compilation: func(ctx context.Context, w digitaltwin.GraphWriter) error {
			return w.RetractNode(ctx, NodeB{})
		},
		graph: snapshot{tree(NodeA{}), tree(NodeC{})},
		checks: []check{
			created(tree(NodeC{})),
			updated(tree(NodeA{})),
			removed(),
		},
	},
	{
		name:     "change-root",
		location: locateSource(),
		compilation: func(ctx context.Context, w digitaltwin.GraphWriter) error {
			return assert.Graph(w).OneToOne(ctx, NodeB{}, NodeC{})
		},
		graph: snapshot{tree(NodeA{}), tree(NodeB{}, NodeC{})},
		checks: []check{
			created(tree(NodeB{}, NodeC{})),
			updated(),
			removed(tree(NodeC{})),
		},
	},
	{
		name:     "merge-trees",
		location: locateSource(),
		compilation: func(ctx context.Context, w digitaltwin.GraphWriter) error {
			return assert.Graph(w).OneToOne(ctx, NodeA{}, NodeB{})
		},
		graph: snapshot{tree(NodeA{}, NodeB{}, NodeC{})},
		checks: []check{
			created(),
			updated(tree(NodeA{}, NodeB{}, NodeC{})),
			removed(tree(NodeB{}, NodeC{})),
		},
	},
	{
		name:     "assert-edge",
		location: locateSource(),
		compilation: func(ctx context.Context, w digitaltwin.GraphWriter) error {
			return w.AssertEdge(ctx, NodeC{}, NodeD{})
		},
		graph: snapshot{tree(NodeA{}, NodeB{}, NodeC{}, NodeD{})},
		checks: []check{
			created(),
			updated(tree(NodeA{}, NodeB{}, NodeC{}, NodeD{})),
			removed(),
		},
	},
	{
		name:     "retract-edges",
		location: locateSource(),
		compilation: func(ctx context.Context, w digitaltwin.GraphWriter) error {
			n, err := w.RetractEdges(ctx, NodeC{}, reflect.TypeOf(NodeD{}))
			if err != nil {
				return err
			}
			if n != 1 {
				return fmt.Errorf("expected 1 edge, got %d", n)
			}
			return nil
		},
		graph: snapshot{tree(NodeA{}, NodeB{}, NodeC{}), tree(NodeD{})},
		checks: []check{
			created(tree(NodeD{})),
			updated(tree(NodeA{}, NodeB{}, NodeC{})),
			removed(),
		},
	},
}

// Run executes a sequence of test cases on a digitaltwin engine using the given
// digitaltwin.Applier and digitaltwin.WhatChangeder interfaces. It verifies that
// the engine correctly applies graph changes and monitors their effects.
//
// We deliberately avoid receiving a contextual argument for each test to ensure
// that the test suite runs under neutral conditions without any external
// influences or timeouts. This approach is consistent across test cases because
// the intention is to test the correctness of operations, not their performance
// or context-dependent behaviours.
//
// The testing process requires all cases to execute in a strict sequence because
// the state of the graph at the end of one test is the starting point for the
// next. This sequential execution is crucial in evaluating whether the state
// progresses correctly over a series of transactions, akin to the real-world use
// of an engine over time.
func Run(t *testing.T, applier digitaltwin.Applier, changeder digitaltwin.WhatChangeder) {
	t.Helper()

	// We deliberately use the background context because this test-suite does not
	// check performance. Also, engine implementations should not depend on specific
	// context values. When this assumption changes, this test-suite will have
	// changes accordingly as well.
	ctx := context.Background()

	// All test-cases run in-order, on the same engine, because each case's graph
	// checks depend on the previous compilations. Otherwise, we would not be able to
	// check the continuity of the engine across time.
	//
	// That is, a test case cannot run if the previous case had failed.
	var lastGraph snapshot
	for _, c := range cases {
		// We encourage developers to read the source code directly, especially when
		// failures are not clear enough. We'd put a lot of effort into making this suite
		// readable and understandable.
		t.Logf("Read the source for test-case %v at %v", c.name, c.location)
		// Test cases begin by applying their compilation using the tested engine.
		err := applier.Apply(ctx, c.compilation)
		if err != nil {
			t.Fatalf("Apply(%v) failed: %v", c.name, err)
		}
		// Then, the tested engine is consulted to detect changes to the graph.
		changes, err := changeder.WhatChanged(ctx)
		if err != nil {
			t.Fatalf("WhatChanged(%v) failed: %v", c.name, err)
		}
		// Each test-case specifies a set of checks to perform against the resulting
		// changes.
		for _, check := range c.checks {
			if problem := check(changes); problem != "" {
				t.Errorf("Check changes of %v: %v", c.name, problem)
			}
		}
		// Regardless of the checks specified for each test-case, there are checks to
		// perform for every graph state.
		for _, check := range c.graph.Checks(lastGraph) {
			if problem := check(changes); problem != "" {
				// This time we do not include the test-case's explanation because these checks
				// already include a different explanation.
				t.Errorf("Check graph of %v: %v", c.name, problem)
			}
		}
		// Finally, we remember the current state of the entire graph to compare against
		// during the next test-case.
		lastGraph = c.graph
	}
}

// We support only narrow trees here to focus on depth progression (sequential
// dependency), which is a common pattern in digital twin modelling, representing
// chained events or dependencies. The emphasis on narrow trees allows the
// function to operate with a predictable pattern of assembly and simplicity in
// connectivity.
func tree(root digitaltwin.Value, children ...digitaltwin.Value) digitaltwin.Assembly {
	var b digitaltwin.AssemblyBuilder
	b.Roots(root)
	// We begin the building pattern with the root as the starting leaf.
	// Subsequently, for each child provided, we connect the current leaf to the
	// child, and the child becomes the new leaf. This iterative process effectively
	// constructs a linked list, narrowing the tree to a simple path from root to the
	// last leaf.
	leaf := root
	for _, child := range children {
		b.Connect(leaf, child)
		leaf = child
	}
	return b.Assemble()
}

// Call this function to set the location of every test-case in the source file.
// The returned string is used to guide developers of digital-twin engines to the
// appropriate test-case.
func locateSource() (path string) {
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		panic("runtime.Caller failed")
	}
	return fmt.Sprintf("%v:%v", file, line)
}
