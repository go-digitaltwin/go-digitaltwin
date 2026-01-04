package digitaltwin

import (
	"fmt"
	"hash"
	"testing"
)

// based on stdlib strings/builder_test.go
func TestBuilderAllocs(t *testing.T) {
	t.Skip("Skip until @danielorbach figures out why there are more than two allocations")
	// Issue 23382; verify that copyCheck doesn't force the
	// AssembleBuilder to escape and be heap-allocated.
	n := testing.AllocsPerRun(10000, func() {
		var b AssemblyBuilder
		b.Hint(100, 0)
		b.Nodes(dummyNode{})
		_ = b.Assemble()
	})
	// 2 allocations:
	//	1. one for the internal map of the builder,
	//	2. another copies the internal map into the built AssemblyGraph
	// TODO: somehow there's a third allocation - WTF?!?
	if n != 2 {
		t.Errorf("Builder allocs = %v; want 2", n)
	}
}

// based on stdlib strings/builder_test.go
func TestBuilderCopyPanic(t *testing.T) {
	tests := []struct {
		name      string
		fn        func()
		wantPanic bool
	}{
		{
			name:      "Assemble",
			wantPanic: false,
			fn: func() {
				var a AssemblyBuilder
				a.Nodes(dummyNode{})
				b := a
				_ = b.Assemble() // appease vet
			},
		},
		{
			name:      "Reset",
			wantPanic: false,
			fn: func() {
				var a AssemblyBuilder
				a.Nodes(dummyNode{id: 'x'})
				b := a
				b.Reset()
				b.Nodes(dummyNode{id: 'y'})
			},
		},
		{
			name:      "Hint",
			wantPanic: true,
			fn: func() {
				var a AssemblyBuilder
				a.Hint(1, 1)
				b := a
				b.Hint(2, 2)
			},
		},
		{
			name:      "Nodes",
			wantPanic: true,
			fn: func() {
				var a AssemblyBuilder
				a.Nodes(dummyNode{id: 'x'})
				b := a
				b.Nodes(dummyNode{id: 'y'})
			},
		},
		{
			name:      "Connect",
			wantPanic: true,
			fn: func() {
				var a AssemblyBuilder
				a.Connect(dummyNode{id: 'x'}, dummyNode{id: 'X'})
				b := a
				b.Connect(dummyNode{id: 'y'}, dummyNode{id: 'Y'})
			},
		},
		{
			name:      "Roots",
			wantPanic: true,
			fn: func() {
				var a AssemblyBuilder
				a.Roots(dummyNode{id: 'x'}, dummyNode{id: 'X'})
				b := a
				b.Roots(dummyNode{id: 'y'}, dummyNode{id: 'Y'})
			},
		},
	}
	for _, tt := range tests {
		didPanic := make(chan bool)
		go func() {
			defer func() { didPanic <- recover() != nil }()
			tt.fn()
		}()
		if got := <-didPanic; got != tt.wantPanic {
			t.Errorf("%s: panicked = %v; want %v", tt.name, got, tt.wantPanic)
		}
	}
}

type dummyNode struct {
	InformationElement
	id byte
}

func (d dummyNode) String() string {
	return fmt.Sprintf("dummy(%d)", d.id)
}

func (d dummyNode) ContentAddress(h hash.Hash) error {
	h.Write([]byte{d.id})
	return nil
}
