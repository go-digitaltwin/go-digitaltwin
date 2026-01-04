/*
Package assert provides syntax sugar for modifying the relationships between
nodes in digital-twin graphs according common patterns. The most common patterns
include one-to-one, one-to-many, many-to-one, and many-to-many associations.

Ensuring that these relationships are correctly established, maintained, and
modified is essential for accurately reflecting real-world interactions and
dependencies within the digital twin model.

The package defines interfaces that allow specific [digitaltwin.GraphWriter]
implementations to specialise for those common patterns. For example, one-to-one
relationship involves two wildcard retractions, which can be omitted if the two
nodes are already connected according to that association's constraints.
*/
package assert

import (
	"context"
	"fmt"
	"reflect"

	"github.com/go-digitaltwin/go-digitaltwin"
)

// Graph extends the given [digitaltwin.GraphWriter]. It returns a type that
// supports additional assertions of the common relationships.
//
// When asserting relationships between two given values, the relation must be
// true for all values of the same type. That is, every source value of type A
// and target value of type B, must always be called with the same relationship
// assertion.
//
// Relationship assertion functions panic if they detect that the graph they are
// operating on violated that constraint before calling them. The graph is not
// directly observed, rather the number of retracted edges hints about the prior
// state of the graph. It is safe to assume that the graph lost its integrity
// because the existence of more edges than allowed by the relationship kind
// violates the above constraint.
func Graph(w digitaltwin.GraphWriter) relationshipWriter {
	return relationshipWriter{w}
}

type relationshipWriter struct {
	digitaltwin.GraphWriter
}

// OneToOne asserts that a strict one-to-one relationship exists between the
// given source and target values.
//
// If the underlying GraphWriter implements the OneToOneAsserter interface, its
// specialised implementation is called instead.
//
// To maintain the one-to-one relationship between the two given values, any
// prior connections are adjusted. Specifically:
//
//   - Edges originating from the given source to any node of the same type as the
//     given target are retracted.
//   - Edges to the given target originating from any node of the same type as
//     the given source are retracted.
//
// If during its operation, the function had retracted too many edges (more than
// one) in any direction, the function panics, according to the constraints
// mentioned on [Graph].
func (a relationshipWriter) OneToOne(ctx context.Context, source, target digitaltwin.Value) error {
	if x, ok := a.GraphWriter.(OneToOneAsserter); ok {
		return x.AssertOneToOne(ctx, source, target)
	}

	edgesFrom, err := a.RetractEdges(ctx, source, reflect.TypeOf(target))
	if err != nil {
		return fmt.Errorf("retract edges from: %w", err)
	} else if edgesFrom > 1 {
		// A one-to-one relationship always maintains at most a single edge originating
		// from the given source to any value of the same type as the given target.
		panic(newGraphIntegrityError("one-to-one", "from source", edgesFrom))
	}

	edgesTo, err := a.RetractEdges(ctx, target, reflect.TypeOf(source))
	if err != nil {
		return fmt.Errorf("retract edges to: %w", err)
	} else if edgesTo > 1 {
		// A one-to-one relationship always maintains at most a single edge to the given
		// target originating from any value of the same type as the given source.
		panic(newGraphIntegrityError("one-to-one", "to target", edgesTo))
	}

	err = a.AssertEdge(ctx, source, target)
	if err != nil {
		return fmt.Errorf("assert edge: %w", err)
	}

	return nil
}

// OneToOneAsserter is the interface implemented by [digitaltwin.GraphWriter]
// types that specialise in asserting one-to-one relationships in digital-twin
// graphs.
//
// Implementations may choose to not revert the modifications made to the graph
// during AssertOneToOne because transaction management (or other equivalent
// rollback mechanisms) is up to the [digitaltwin.Applier].
type OneToOneAsserter interface {
	// AssertOneToOne returns a nil error after it had successfully asserted that:
	//
	//  - There's only a single edge originating from the given source to any node of
	//    the same type as the given target.
	//	- There's only a single edge to the given target originating from any node of
	//	  the same type as the given source.
	//	- That single edge connects the given source and the given target.
	//
	// Otherwise, it returns a non-nil error and the graph may have been partially
	// modified. Callers should be aware of that and manage rollback on their own.
	AssertOneToOne(ctx context.Context, source, target digitaltwin.Value) error
}

// OneToMany asserts that a strict one-to-many relationship exists between the
// given source and target values.
//
// If the underlying GraphWriter implements the OneToManyAsserter interface, its
// specialised implementation is called instead.
//
// To maintain the one-to-many relationship between the two given values, some
// prior connections are adjusted. Specifically:
//
//   - Edges originating from the given source to any node of the same type as the
//     given target are retained.
//   - Edges to the given target originating from any node of the same type as
//     the given source are retracted.
//
// If during its operation, the function had retracted too many edges (more than
// one) to the target value, the function panics, according to the constraints
// mentioned on [Graph].
func (a relationshipWriter) OneToMany(ctx context.Context, source, target digitaltwin.Value) error {
	if x, ok := a.GraphWriter.(OneToManyAsserter); ok {
		return x.AssertOneToMany(ctx, source, target)
	}

	edgesTo, err := a.RetractEdges(ctx, target, reflect.TypeOf(source))
	if err != nil {
		return fmt.Errorf("retract edges to: %w", err)
	} else if edgesTo > 1 {
		// A one-to-many relationship always maintains at most a single edge to the given
		// target originating from any value of the same type as the given source.
		panic(newGraphIntegrityError("one-to-many", "to target", edgesTo))
	}

	err = a.AssertEdge(ctx, source, target)
	if err != nil {
		return fmt.Errorf("assert edge: %w", err)
	}

	return nil
}

// OneToManyAsserter is the interface implemented by [digitaltwin.GraphWriter]
// types that specialise in asserting one-to-many relationships in digital-twin
// graphs.
//
// Implementations may choose to not revert the modifications made to the graph
// during AssertOneToOne because transaction management (or other equivalent
// rollback mechanisms) is up to the [digitaltwin.Applier].
type OneToManyAsserter interface {
	// AssertOneToMany returns a nil error after it had successfully asserted that:
	//
	//	- There's only a single edge to the given target originating from any node of
	//	  the same type as the given source.
	//	- That single edge connects the given source and the given target.
	//
	// Otherwise, it returns a non-nil error and the graph may have been partially
	// modified. Callers should be aware of that and manage rollback on their own.
	AssertOneToMany(ctx context.Context, source, target digitaltwin.Value) error
}

// ManyToOne asserts that a strict many-to-one relationship exists between the
// given source and target values.
//
// If the underlying GraphWriter implements the ManyToOneAsserter interface, its
// specialised implementation is called instead.
//
// To maintain the many-to-one relationship between the two given values, some
// prior connections are adjusted. Specifically:
//
//   - Edges originating from the given source to any node of the same type as the
//     given target are retracted.
//   - Edges to the given target originating from any node of the same type as
//     the given source are retained.
//
// If during its operation, the function had retracted too many edges (more than
// one) from the source value, the function panics, according to the constraints
// mentioned on [Graph].
func (a relationshipWriter) ManyToOne(ctx context.Context, source, target digitaltwin.Value) error {
	if x, ok := a.GraphWriter.(ManyToOneAsserter); ok {
		return x.AssertManyToOne(ctx, source, target)
	}

	edgesFrom, err := a.RetractEdges(ctx, source, reflect.TypeOf(target))
	if err != nil {
		return fmt.Errorf("retract edges from: %w", err)
	} else if edgesFrom > 1 {
		// A many-to-one relationship always maintains at most a single edge originating
		// from the given source to any value of the same type as the given target.
		panic(newGraphIntegrityError("many-to-one", "from source", edgesFrom))
	}

	err = a.AssertEdge(ctx, source, target)
	if err != nil {
		return fmt.Errorf("assert edge: %w", err)
	}

	return nil
}

// ManyToOneAsserter is the interface implemented by [digitaltwin.GraphWriter]
// types that specialise in asserting many-to-one relationships in digital-twin
// graphs.
//
// Implementations may choose to not revert the modifications made to the graph
// during AssertOneToOne because transaction management (or other equivalent
// rollback mechanisms) is up to the [digitaltwin.Applier].
type ManyToOneAsserter interface {
	// AssertManyToOne returns a nil error after it had successfully asserted that:
	//
	//  - There's only a single edge originating from the given source to any node of
	//    the same type as the given target.
	//	- That single edge connects the given source and the given target.
	//
	// Otherwise, it returns a non-nil error and the graph may have been partially
	// modified. Callers should be aware of that and manage rollback on their own.
	AssertManyToOne(ctx context.Context, source, target digitaltwin.Value) error
}

// ManyToMany asserts that a strict many-to-many relationship exists between the
// given source and target values.
//
// If the underlying GraphWriter implements the ManyToManyAsserter interface, its
// specialised implementation is called instead.
//
// To maintain the many-to-many relationship between the two given values, no
// prior connections are adjusted. Specifically:
//
//   - Edges originating from the given source to any node of the same type as the
//     given target are retained.
//   - Edges to the given target originating from any node of the same type as
//     the given source are retained.
//
// This function does not panic due to the flexibility of the many-to-many
// relationship.
func (a relationshipWriter) ManyToMany(ctx context.Context, source, target digitaltwin.Value) error {
	if x, ok := a.GraphWriter.(ManyToManyAsserter); ok {
		return x.AssertManyToMany(ctx, source, target)
	}

	err := a.AssertEdge(ctx, source, target)
	if err != nil {
		return fmt.Errorf("assert edge: %w", err)
	}

	return nil
}

// ManyToManyAsserter is the interface implemented by [digitaltwin.GraphWriter]
// types that specialise in asserting many-to-many relationships in digital-twin
// graphs.
//
// Implementations may choose to not revert the modifications made to the graph
// during AssertOneToOne because transaction management (or other equivalent
// rollback mechanisms) is up to the [digitaltwin.Applier].
type ManyToManyAsserter interface {
	// AssertManyToMany returns a nil error after it had successfully asserted an
	// edge connects the given source and the given target.
	//
	// Otherwise, it returns a non-nil error and the graph may have been partially
	// modified. Callers should be aware of that and manage rollback on their own.
	AssertManyToMany(ctx context.Context, source, target digitaltwin.Value) error
}

// Assertions call newGraphIntegrityError when the graph structure violates the
// expected relationships between nodes, indicating an inconsistency in the
// graph's state.
//
// The returned error is panicked to enforce developer consistency when asserting
// the strict relationships (one-to-one, one-to-many, or many-to-one), where the
// number of retracted edges between nodes suggests the graph lost its integrity,
// probably due to developer misuse (e.g. asserting different relationships in
// different compilations).
//
// This function expects the relationship argument to be one of "one-to-one",
// "one-to-many", "many-to-one".
//
// This function expects the direction argument to be one of "from source" or "to
// target".
func newGraphIntegrityError(relationship, direction string, affectedEdges int) error {
	switch relationship {
	case "one-to-one", "one-to-many", "many-to-one":
	default:
		panic("github.com/go-digitaltwin/go-digitaltwin/assert: unknown relationship: " + relationship)
	}
	switch direction {
	case "from source", "to target":
	default:
		panic("github.com/go-digitaltwin/go-digitaltwin/assert: unknown direction: " + direction)
	}
	return fmt.Errorf("inconsistent graph detected: relationship %v was violated with %v affected edges %v", relationship, affectedEdges, direction)
}
