package digitaltwin

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"maps"
	"sync"

	"github.com/danielorbach/go-component"
	"gocloud.dev/pubsub"
)

// An AttributeFunc is a function that defines a specific attribute of
// digital-twin assemblies. For a given Assembly, it returns the attribute's
// value and a bool indicating whether that attribute is valid for that Assembly.
//
// It usually visits the given Assembly to extract the appropriate value from it,
// but any value of type V is appropriate.
type AttributeFunc[V any] func(assembly Assembly) (V, bool)

// AttributeMap correlates between assemblies of a digital-twin graph and their
// corresponding attribute value. The generic parameter V denotes the type of the
// attribute's value.
//
// Use the map's Update and Find methods to modify and access the stored
// attribute values by a ComponentID.
//
// AttributeMap is designed to be concurrently safe and can be accessed by multiple
// goroutines simultaneously.
type AttributeMap[V any] struct {
	m           map[ComponentID]V
	mu          sync.Mutex
	attributeOf AttributeFunc[V]
}

// NewAttributeMap returns a mapping/view of a single attribute from a
// digital-twin assembly. The provided attr function defines the desired
// attribute to store for every Assembly.
//
// If an existing map 'm' is provided to NewAttributeMap, it will be used;
// otherwise, a new empty map is initialized. Note that the type of 'm'
// should correspond to the type expected by the attr function.
func NewAttributeMap[V any](attr AttributeFunc[V], m map[ComponentID]V) AttributeMap[V] {
	newMap := make(map[ComponentID]V)
	if m != nil {
		maps.Copy(newMap, m)
	}

	return AttributeMap[V]{
		m:           newMap,
		attributeOf: attr,
	}
}

// Find looks up the given ComponentID and returns its last known attribute
// value. If the given ComponentID cannot be found, Find indicates that by
// returning ok == false.
//
// Find is safe for concurrent use.
func (a *AttributeMap[V]) Find(id ComponentID) (v V, ok bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	v, ok = a.m[id]
	return v, ok
}

// Update determines the effective value of the mapped attribute based on the
// given Assembly.
//
// If the Assembly's attribute value is deemed invalid, this function will expunge
// the Assembly from the AttributeMap. In cases where the Assembly is not
// previously registered within the map, the Update function becomes a no-op, and
// the map is left unmodified.
//
// Update is safe for concurrent use.
func (a *AttributeMap[V]) Update(assembly Assembly) {
	a.mu.Lock()
	defer a.mu.Unlock()
	v, ok := a.attributeOf(assembly)
	if ok {
		a.m[assembly.AssemblyID()] = v
	} else {
		// We are expunging the stored attribute value as it was deemed invalid by the
		// attribute function for the assembly at hand. We cannot keep the previous value
		// (if any) because of the definition of an "invalid" attribute for a specific
		// assembly (see comment on AttributeFunc)
		delete(a.m, assembly.AssemblyID())
	}
}

// Iter applies the provided function 'fn' to each assembly and its
// associated attribute. Iteration continues until 'fn' returns false,
// or once all assemblies have been visited.
func (a *AttributeMap[V]) Iter(fn func(k ComponentID, v V) bool) {
	// TODO: Need to be refactor when go 1.22 releases with support for range over
	// func. see https://github.com/golang/go/issues/61405
	for k, v := range a.m {
		if !fn(k, v) {
			break
		}
	}
}

// TrackAttribute return a component.Proc that tracks GraphChanged notifications
// of a digit-twin and maintains an up-to-date view of attribute values for the
// observed assemblies in its graph. The tracked attribute is defined by the
// provided AttributeMap.
//
// This procedure runs sequentially of GraphChanged message and updates the given
// AttributeMap one assembly at a time. Use the Find method of AttributeMap to
// receive the attribute a specific assembly.
func TrackAttribute[V any](m *AttributeMap[V], source *pubsub.Subscription) component.Proc {
	return func(l *component.L) {
		var trackedGraph ForestHash
		for l.Continue() {
			msg, err := source.Receive(l.GraceContext())
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				l.Errorf("receive: %v", err)
				continue
			}
			var graphChanged GraphChanged
			dec := gob.NewDecoder(bytes.NewReader(msg.Body))
			if err := dec.Decode(&graphChanged); err != nil {
				l.Fatalf("Failed to unmarshal graph changes; stopping attribute tracking: %v\n", err)
			}

			if trackedGraph != (ForestHash{}) && trackedGraph != graphChanged.GraphBefore {
				l.Logf("Detected a discontinuity in GraphChanged messages: last handled graph hash %s, received previous graph hash %s",
					trackedGraph.String(), graphChanged.GraphBefore.String())
				l.Fatalf("Exiting due to detected discontinuity")
			}

			for _, created := range graphChanged.Created {
				m.Update(created)
			}
			for _, updated := range graphChanged.Updated {
				m.Update(updated)
			}
			msg.Ack()
		}
	}
}
