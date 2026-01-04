package compilation_test

import (
	"context"
	"encoding/gob"
	"fmt"
	"reflect"

	"github.com/go-digitaltwin/go-digitaltwin"
	"github.com/go-digitaltwin/go-digitaltwin/compilation"
)

// We demonstrate how to use the Recorder to capture, replay, and manage graph
// mutations. It shows the complete workflow from recording operations to
// executing them on a graph, including encoding and decoding steps for
// transmission across process boundaries, while highlighting the defensive copy
// behaviour of the Steps method.
func ExampleRecorder() {
	// Initialise a recorder to capture graph mutations.
	var recorder compilation.Recorder

	// Define test nodes representing domain entities.
	nodeA := TestNode{Value: "A"}
	nodeB := TestNode{Value: "B"}
	nodeC := TestNode{Value: "C"}

	// Build a sequence of graph mutations.
	fmt.Println("Recording steps:")
	recorder.AssertNode(nodeA)
	recorder.AssertNode(nodeB)
	recorder.AssertEdge(nodeA, nodeB)
	recorder.RetractNode(nodeC)
	recorder.RetractEdges(nodeA, reflect.TypeOf(TestNode{}))

	// Retrieve the recorded mutation steps.
	steps := recorder.Steps()
	fmt.Printf("Recorded %d steps\n", len(steps))

	encodedSteps, err := compilation.Encode(steps)
	if err != nil {
		panic(fmt.Sprintf("Failed to encode steps: %v", err))
	}

	// In a distributed scenario, the encoded bytes would be transmitted to another
	// process. Here we simulate that by simply using the encoded bytes directly.

	// Decode the steps in the "receiving" process.
	fmt.Println("\nDecoding steps in receiving process:")
	decodedSteps, err := compilation.Decode(encodedSteps)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Decoded %d steps\n", len(decodedSteps))

	// Transform the decoded steps into an executable compilation.
	fmt.Println("\nReplaying decoded steps:")
	c := compilation.Replay(decodedSteps)

	// Prepare a writer that prints operations directly to stdout.
	writer := PrintGraphWriter{}

	// Execute the decoded compilation against our graph writer.
	err = c(context.Background(), writer)
	if err != nil {
		panic(err)
	}

	// Demonstrate the defensive copy behaviour of Steps().
	steps = nil
	fmt.Printf("\nModified steps length: %d\n", len(steps))
	fmt.Printf("Original recorder steps length: %d\n", len(recorder.Steps()))

	// Clear the recorder and verify its state.
	recorder.Reset()
	fmt.Printf("\nSteps after reset: %d\n", len(recorder.Steps()))

	// Confirm the recorder remains functional after reset.
	recorder.AssertNode(nodeA)
	fmt.Printf("Steps after recording a new step: %d\n", len(recorder.Steps()))

	// Output:
	// Recording steps:
	// Recorded 5 steps
	//
	// Decoding steps in receiving process:
	// Decoded 5 steps
	//
	// Replaying decoded steps:
	// + (A)
	// + (B)
	// (A) -> (B)
	// - (C)
	// (A) <-/-> compilation_test.TestNode
	//
	// Modified steps length: 0
	// Original recorder steps length: 5
	//
	// Steps after reset: 0
	// Steps after recording a new step: 1
}

// A TestNode represents a domain entity in the graph for demonstration purposes.
// It provides a simple value-based node type for testing graph operations.
type TestNode struct {
	digitaltwin.InformationElement
	Value string
}

func (n TestNode) String() string {
	return fmt.Sprintf("(%s)", n.Value)
}

func init() {
	// Register the TestNode type with gob to enable serialisation.
	gob.Register(TestNode{})
}

// A PrintGraphWriter implements the digitaltwin.GraphWriter interface by
// directly printing graph operations to stdout, making it useful for example
// tests.
type PrintGraphWriter struct{}

func (w PrintGraphWriter) AssertNode(ctx context.Context, node digitaltwin.Value) error {
	fmt.Println("+", node)
	return nil
}

func (w PrintGraphWriter) RetractNode(ctx context.Context, node digitaltwin.Value) error {
	fmt.Println("-", node)
	return nil
}

func (w PrintGraphWriter) AssertEdge(ctx context.Context, from, to digitaltwin.Value) error {
	fmt.Println(from, "->", to)
	return nil
}

func (w PrintGraphWriter) RetractEdges(ctx context.Context, node digitaltwin.Value, kind reflect.Type) (int, error) {
	fmt.Println(node, "<-/->", kind)
	return 0, nil
}

// We demonstrate the usage of the Recorder for capturing and replaying a series
// of graph relationship assertions. This example covers the entire lifecycle:
// defining nodes relevant to a specific scenario, recording various types of
// relationship assertions (OneToOne, OneToMany, ManyToOne, ManyToMany), encoding
// these recorded steps for potential transmission, decoding them back, and
// replaying the steps onto a graph writer.
func ExampleRecorder_relationshipAssertions() {
	// Initialise a new Recorder. The Recorder will capture all graph mutation
	// operations called on it.
	var recorder compilation.Recorder

	// Define a set of nodes representing entities for this example scenario.

	// OneToOne: A person (Alice) and their unique passport.
	alice := TestNode{Value: "Alice (Person)"}
	alicePassport := TestNode{Value: "Alice's Passport"}

	// OneToMany: A company (OneLayer) and its employees (Bob, Charlie).
	oneLayer := TestNode{Value: "OneLayer (Company)"}
	bob := TestNode{Value: "Bob (Employee)"}
	charlie := TestNode{Value: "Charlie (Employee)"}

	// ManyToOne: Multiple reports (Q1, Q2) managed by a single person (Alice).
	q1Report := TestNode{Value: "Q1 Report"}
	q2Report := TestNode{Value: "Q2 Report"}

	// ManyToMany: Employees (Bob, Charlie) and the skills they possess (Go, Python).
	goSkill := TestNode{Value: "Go Programming (Skill)"}
	pythonSkill := TestNode{Value: "Python Programming (Skill)"}

	// Record a sequence of relationship assertions. Each call to an AssertXxxYyy
	// method on the recorder adds a corresponding step to its internal list. These
	// steps represent the intended graph mutations.
	fmt.Println("Recording relationship assertion steps:")
	recorder.AssertOneToOne(alice, alicePassport)
	recorder.AssertOneToMany(oneLayer, bob)
	recorder.AssertOneToMany(oneLayer, charlie)
	recorder.AssertManyToOne(q1Report, alice) // Alice manages the Q1 Report.
	recorder.AssertManyToOne(q2Report, alice) // Alice also manages the Q2 Report.
	recorder.AssertManyToMany(bob, goSkill)
	recorder.AssertManyToMany(charlie, goSkill)
	recorder.AssertManyToMany(bob, pythonSkill) // Bob also possesses Python skill.

	// Retrieve the recorded relationship assertion steps.
	steps := recorder.Steps()
	fmt.Printf("Recorded %d relationship steps\n", len(steps))

	// Encode the steps for transmission or storage.
	encodedSteps, err := compilation.Encode(steps)
	if err != nil {
		panic(err)
	}

	// Decode the steps, simulating receipt in another process.
	fmt.Println("\nDecoding relationship steps:")
	decodedSteps, err := compilation.Decode(encodedSteps)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Decoded %d relationship steps\n", len(decodedSteps))

	// Transform decoded steps into an executable compilation.
	fmt.Println("\nReplaying decoded relationship steps:")
	c := compilation.Replay(decodedSteps)

	// Prepare a writer to observe the replayed operations.
	writer := PrintGraphWriter{}
	err = c(context.Background(), writer)
	if err != nil {
		panic(err)
	}
	// Output:
	// Recording relationship assertion steps:
	// Recorded 8 relationship steps
	//
	// Decoding relationship steps:
	// Decoded 8 relationship steps
	//
	// Replaying decoded relationship steps:
	// (Alice (Person)) <-/-> compilation_test.TestNode
	// (Alice's Passport) <-/-> compilation_test.TestNode
	// (Alice (Person)) -> (Alice's Passport)
	// (Bob (Employee)) <-/-> compilation_test.TestNode
	// (OneLayer (Company)) -> (Bob (Employee))
	// (Charlie (Employee)) <-/-> compilation_test.TestNode
	// (OneLayer (Company)) -> (Charlie (Employee))
	// (Q1 Report) <-/-> compilation_test.TestNode
	// (Q1 Report) -> (Alice (Person))
	// (Q2 Report) <-/-> compilation_test.TestNode
	// (Q2 Report) -> (Alice (Person))
	// (Bob (Employee)) -> (Go Programming (Skill))
	// (Charlie (Employee)) -> (Go Programming (Skill))
	// (Bob (Employee)) -> (Python Programming (Skill))
}

// We demonstrate how Targets extracts the unique set of nodes that will be
// affected by a compilation. This function is essential for pre-execution
// analysis: determining which nodes need to be locked, validating permissions,
// or optimising batch operations of the affected nodes.
func ExampleTargets() {
	// Create a recorder to capture graph mutations.
	var recorder compilation.Recorder
	// Define nodes representing different entities in our domain.
	var (
		nodeA = TestNode{Value: "A"}
		nodeB = TestNode{Value: "B"}
		nodeC = TestNode{Value: "C"}
		nodeD = TestNode{Value: "D"}
		nodeE = TestNode{Value: "E"}
	)

	// Build a complex set of steps where nodes appear multiple times across
	// different operation types.
	recorder.AssertNode(nodeA)
	recorder.AssertNode(nodeA) // Duplicate - but Targets yields each node only once.
	recorder.AssertEdge(nodeA, nodeB)
	recorder.AssertEdge(nodeB, nodeC)
	recorder.RetractNode(nodeC)
	recorder.RetractEdges(nodeB, reflect.TypeOf(TestNode{})) // We DO NOT know about any nodes affected by wildcard retractions.
	recorder.AssertOneToOne(nodeC, nodeD)
	recorder.AssertOneToMany(nodeD, nodeE)
	recorder.AssertManyToOne(nodeE, nodeA)
	recorder.AssertManyToMany(nodeA, nodeE)

	steps := recorder.Steps()
	// The Targets function provides a deduplicated view of all nodes that will be
	// touched by the compilation, regardless of:
	//	- How many times a node appears in the steps
	//	- What type of operations affect the node
	//	- Whether the node is a source, target, or both in relationships
	//
	// This enables pre-processing capabilities, such as
	//	- Locking only the affected nodes before execution
	//	- Validating that all target nodes meet preconditions
	//	- Computing the minimal subgraph that needs to be loaded
	fmt.Println("Unique nodes affected by compilation steps:")
	for target := range compilation.Targets(steps) {
		fmt.Println(target)
	}

	// Unordered output:
	// Unique nodes affected by compilation steps:
	// (A)
	// (B)
	// (C)
	// (D)
	// (E)
}
