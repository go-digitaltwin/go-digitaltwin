package digitaltwin_test

import (
	"context"
	"encoding/gob"
	"fmt"

	"github.com/danielorbach/go-component"
	"github.com/danielorbach/go-component/loader"
	"github.com/go-digitaltwin/go-digitaltwin"
)

// First, we define two node types: Person and Dog.
// These types will be used to demonstrate nodes in our exemplar graph.

// All node types are named structs that embed InformationElement.
type Person struct {
	// Always embed this type to implement Value.
	digitaltwin.InformationElement
	// Add your fields here (as many as you see fit).
	Name string
	Age  int
}

// The String method returns a human-readable representation of the node.
func (p Person) String() string {
	return fmt.Sprintf("(%s/%d)", p.Name, p.Age)
}

// The name of the type is the label of the node in the graph (see
// digitaltwin/engine package).
type Dog struct {
	// This is an experimental decision we are still evaluating.
	digitaltwin.InformationElement
	// Different nodes may have the same properties.
	Name string
}

// Use it often to make debug logs of the graph types more readable.
func (d Dog) String() string {
	return fmt.Sprintf("(%s)", d.Name)
}

// Remember the node types must be registered with gob before they can be
// used with the digitaltwin API.
func init() {
	// It doesn't matter where you register the types, as long as it's before
	// you use them.
	gob.Register(Person{})
	// You can register a node type with a different label than its name.
	gob.RegisterName("Doggo", Dog{})
}

//=============================================================================

// Next, we create a component.Descriptor that will be used to bootstrap our
// exemplar DigitalTwin instance.

// Component describes an exemplar component deployment in the Atmosphere
// ecosystem.
//
// For this example, we will omit most of its fields - do not omit them in your
// own components.
var Component = component.Descriptor{
	Name: "ExampleComponent",
	// ...
	Bootstrap: func(l *component.L, linker component.Linker, options any) error {
		// A digital-twin is composed of two concurrent processes: applying modifications
		// to the graph and periodically publishing changes.
		//
		// Uniquely for this example, these processes will happen sequentially, and
		// publishing goes to stdout instead of a message target.

		// First, let's apply trivial knowledge to the graph: Baz is 42 years old and has
		// a dog he calls Riderman.
		var (
			baz      = Person{Name: "Baz", Age: 42}
			riderman = Dog{Name: "Riderman"}
		)
		var applier printApplier // We can safely ignore the error of a printApplier.
		_ = applier.Apply(context.Background(), func(ctx context.Context, w digitaltwin.GraphWriter) error {
			return w.AssertEdge(ctx, baz, riderman)
		})

		// Then, let's see what had changed in the graph and publish those changes to
		// stdout.
		//
		// This example never runs, so we don't bother fabricating a functional
		// WhatChangeder.
		var whatChangeder digitaltwin.WhatChangeder
		changes, err := whatChangeder.WhatChanged(l.Context())
		if err != nil {
			panic(err)
		}
		digitaltwin.FormatChanges(changes, "")

		// Once all the component's subcomponents have started, we return from Bootstrap
		// to indicate to the caller (manager/loader/whatever) that the component is
		// ready and executing.
		return nil
	},
}

//=============================================================================

// Finally, we load the component descriptor as part of an executable's main()
// function using component.EntrypointProc (see the component package for more
// details).

func ExampleDigitalTwin_component() {
	loader.ParseFlags(&Component)
	// A deployable executable must know how to load its component descriptors.
	//
	// For this example, leave that part to your imagination.
}
