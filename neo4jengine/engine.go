package neo4jengine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/danielorbach/go-component"
	"github.com/go-digitaltwin/go-digitaltwin"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Engine provides the basic operations required to maintain a digital-twin graph
// on Neo4j.
//
// It applies compilations to the underlying Neo4j graph database. Each
// compilation executes in its own transaction, which is rolled back should the
// compilation fail. This ensures each compilation applies atomically. Each
// compilation is called with a [digitaltwin.GraphWriter] that is scoped to the
// respective transaction.
//
// It returns changesets containing the amalgamation of the applied modifications
// (i.e. calls to Apply) between calls to WhatChanged. To facilitate that
// behaviour, the engine keeps a snapshot (mapping from component-id to
// component-hash) of all disjoint graph components it had observed up to the
// last call to WhatChanged. NewEngine configures the initial value of that
// internal snapshot.
type Engine struct {
	driver   neo4j.DriverWithContext // Connection to the neo4j server/cluster.
	database string                  // Target database name that identifies the specific underlying neo4j graph.
	snapshot snapshot

	taintedNodes nodeMap // Maps digitaltwin.NodeHash to RawNode for tracking changes of disjoint graph components.
	// Ensures multiple concurrent write transactions can safely modify the Neo4j
	// graph, while read transactions get an exclusive lock to maintain data
	// integrity.
	txMutex graphWRMutex
}

// A nodeMap stores the tainted nodes of disjoint graph components that were
// modified during a compilation.
//
// The zero-value nodeMap is ready for use.
//
// A nodeMap is safe for concurrent-use.
type nodeMap struct {
	m  map[digitaltwin.NodeHash]RawNode
	mu sync.Mutex
}

// Taint marks the given RawNodes as "dirty", storing them for later use by
// calling ClearTaints.
//
// If a node is already "dirty", its value is updated. A node is uniquely
// identified by its content-address.
func (t *nodeMap) Taint(nodes ...RawNode) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// Make the zero-value meaningful.
	if t.m == nil {
		t.m = make(map[digitaltwin.NodeHash]RawNode)
	}
	for _, node := range nodes {
		t.m[node.ContentAddress] = node
	}
}

// ClearTaints returns the "dirty" nodes, as marked by prior calls to Taint, and
// "cleans" the nodeMap. So, further calls to ClearTaints without calling Taint
// return an empty slice.
func (t *nodeMap) ClearTaints() []RawNode {
	t.mu.Lock()
	defer t.mu.Unlock()
	// Shortcut, do nothing.
	if t.m == nil {
		return nil
	}
	// We need to both return the marked nodes and clear the internal memory.
	nodes := make([]RawNode, 0, len(t.m))
	for _, node := range t.m {
		nodes = append(nodes, node)
	}
	t.m = nil
	return nodes
}

// NewEngine returns a ready-to-use Engine using the given database as the
// underlying neo4j graph.
//
// The function initialises the Engine with a snapshot of the current disjoint
// graph components in the given graph. In the future, we plan to enable callers
// to replace this (potentially expensive) initialisation with an externally
// composed snapshot.
func NewEngine(ctx context.Context, driver neo4j.DriverWithContext, database string) (*Engine, error) {
	s, err := captureSnapshot(ctx, driver, database)
	if err != nil {
		return nil, fmt.Errorf("capture initial snapshot: %w", err)
	}
	return &Engine{
		driver:   driver,
		database: database,
		snapshot: s,
	}, nil
}

// WhatChanged reviews the entire graph to create a map of its disjoint graph
// components. This allows detecting any new components that have appeared, any
// existing ones that have changed, and any that are no longer there (i.e. merged
// to create larger graph components, or deleted).
//
// Once it has finished its detailed sweep, WhatChanged returns the changes it
// has detected. It lists all the new, updated, and removed graph components,
// providing a full copy of all changed assemblies since the last call.
//
// Before returning, the function updates its internal records to keep a snapshot
// of the disjoint graph components that is up to date with this review. If an
// error occurs during the sweep, the function does not update its internal
// records so that the next call runs as if the failed execution had never been
// called.
func (e *Engine) WhatChanged(ctx context.Context) (changes digitaltwin.GraphChanged, err error) {
	ctx, span := tracer.Start(ctx, "WhatChanged", trace.WithAttributes(
		attribute.String("neo4j.database", e.database),
	))
	defer span.End()
	logger := component.Logger(ctx).With("neo4j.database", e.database)
	ctx = component.InjectLogger(ctx, logger) // Inject for further logs down the call-stack.

	taints, assemblies, err := e.fetchTaintedAssemblies(ctx)
	if err != nil {
		return digitaltwin.GraphChanged{}, fmt.Errorf("fetch tainted assemblies: %w", err)
	}

	// While iterating the disjoint graph components, we must store all assemblies
	// that have changed between the previously stored and the currently fetched
	// snapshots.
	//
	// We use these assemblies to populate the GraphChanged result later. While
	// populating the result, we access the assemblies by their id, so we store them
	// in a map for convenient random access.
	var changedAssemblies = make(map[digitaltwin.ComponentID]digitaltwin.Assembly)

	// Look further down this function for what we do when a rootless assembly is
	// found; the comments there link to the GitHub bug.
	var rootlessAssemblies int

	// We iterate over all disjoint graph components while building a new snapshot of
	// the graph.
	next := make(snapshot)
	for _, a := range assemblies {
		// Add the assembly to the new snapshot.
		next[a.AssemblyID()] = a.AssemblyHash()
		// If the stored snapshot does not contain the assembly (either because it is new
		// or has changed), then add it to the list of changed assemblies.
		if !e.snapshot.ContainsAssembly(a) {
			changedAssemblies[a.AssemblyID()] = a
		}

		// Detect if any assembly lacks a root. We do not terminate the inspection here
		// because we want to collect all assemblies that lack a root. The handling of
		// assemblies is done after all the assemblies have been inspected.
		if len(a.Roots()) == 0 {
			rootlessAssemblies++
		}
	}

	dirtyRoots := make([]digitaltwin.ComponentID, len(taints))
	for i, n := range taints {
		id, err := componentID(n)
		if err != nil {
			// The following error string is not typical. Here's an example:
			//
			//  IMSI component from node(abc..def): inner error...
			return digitaltwin.GraphChanged{}, fmt.Errorf("%v component from %v: %w", n.Label, n.ContentAddress, err)
		}
		dirtyRoots[i] = id
	}

	// Diff snapshots to find out what has changed.
	created, updated, removed := e.snapshot.PartialDiff(next, dirtyRoots)
	// Now, we have all the information we need to populate the GraphChanged result.
	changes.GraphBefore = e.snapshot.GraphHash()
	changes.Timestamp = time.Now().UTC()

	for _, id := range created {
		// Since created assemblies were not in the previous snapshot, we have already
		// stored them in the `changedAssemblies` map while iterating the graph above.
		changes.Created = append(changes.Created, digitaltwin.AssemblyCreated{Assembly: changedAssemblies[id]})
	}
	for _, id := range updated {
		// Since updated assemblies had a different hash in the previous snapshot, we
		// have already stored them in the `changedAssemblies` map while iterating the
		// graph above. We also know their previous hash from the previous snapshot.
		changes.Updated = append(changes.Updated, digitaltwin.AssemblyUpdated{Baseline: e.snapshot[id], Assembly: changedAssemblies[id]})
	}
	for _, id := range removed {
		// Since removed assemblies were in the previous snapshot but not in the current
		// snapshot, we know their hash from the previous snapshot.
		changes.Removed = append(changes.Removed, digitaltwin.AssemblyRemoved{ID: id, Hash: e.snapshot[id]})
	}

	// If during iterating the graph, we've stumbled upon assembly without a root,
	// then this GraphChanged notification becomes invalid, and we return an error.
	//
	// Thus, we make sure that the current snapshot will not be updated, so calling
	// WhatChanged again may recover. We do not know why some queries return rootless
	// assemblies, but we assume this is recoverable.
	if rootlessAssemblies > 0 {
		trace.SpanFromContext(ctx).RecordError(errFoundRootlessAssemblies, trace.WithAttributes(
			attribute.Int("changeset.rootless", rootlessAssemblies),
			attribute.String("changeset.pretty", digitaltwin.FormatChanges(changes, "")),
		))
		rootlessAssemblyCounter.Add(ctx, int64(rootlessAssemblies), metric.WithAttributes(
			attribute.String("neo4j.database", e.database),
		))
		return changes, errFoundRootlessAssemblies
	}

	// Before returning, we don't forget to update the previously stored snapshot for
	// the next time this function is called.
	e.snapshot.Update(changes)
	// As we handle partial snapshots, we must derive GraphAfter from the complete
	// snapshot. This comprehensive state, GraphAfter, reflects the graph following
	// the most recent updates. Therefore, the calculation should occur post the
	// snapshot update.
	changes.GraphAfter = e.snapshot.GraphHash()

	return changes, nil
}

// WhatChanged calls fetchTaintedAssemblies to exclusively read the graph,
// without side effects from concurrent write-transactions (calls to Apply).
//
// It uses the internal taintedNodes structure to "atomically" fetch all
// assemblies that were modified by prior calls to Apply since the last call to
// WhatChanged. We say "atomically" in the sense that the returned taints and
// assemblies are a single unit.
func (e *Engine) fetchTaintedAssemblies(ctx context.Context) (taints []RawNode, assemblies []digitaltwin.Assembly, err error) {
	// We open a new session for every query cycle to ensure transactional isolation
	// and to prevent any state carryover between different query executions.This
	// practice enhances robustness because any session-specific errors or resources
	// are contained and do not affect subsequent operations.
	s := e.driver.NewSession(ctx, neo4j.SessionConfig{
		DatabaseName: e.database,
		AccessMode:   neo4j.AccessModeRead,
	})
	defer func() {
		if err := s.Close(ctx); err != nil {
			component.Logger(ctx).Error("Failed to close session", "error", err, "mode", "read")
		}
	}()

	// Acquire an exclusive lock before starting the graph read operation to ensure
	// that the graph state remains consistent and is not being modified by
	// concurrent write transactions. See graphWRMutex documentation for more
	// information.
	e.txMutex.Lock()
	// Release the exclusive lock to allow to write transactions to proceed now that
	// the graph read operation is complete.
	defer e.txMutex.Unlock()

	// We take a snapshot of all the nodes that were tainted up to this point in
	// time. This ensures that we consider all assemblies that may have changed in
	// prior graph write operations.
	//
	// The taints are cleared from the taintMap to prepare for the next call to
	// WhatChanged.
	taints = e.taintedNodes.ClearTaints()

	assemblies, err = fetchPartialAssemblies(ctx, s, taints)
	if err != nil {
		return nil, nil, err
	}
	return taints, assemblies, nil
}

// This error is returned from Engine.WhatChanged when any rootless assemblies
// are found during a sweep.
var errFoundRootlessAssemblies = errors.New("found rootless assemblies while sweeping the graph")

// Apply opens a new transaction and passes a [digitaltwin.GraphWriter] that
// executes Cypher queries within that transaction to the given compilation.
//
// If the compilation returns a non-nil error, the transaction is rolled back and
// the error is returned to the caller of Apply. In which case, the underlying
// graph database is not modified, as if the compilation was never executed.
//
// The function panics in two scenarios:
//
//   - The underlying graph has been corrupted. This is detected by the
//     compilation-specific digitaltwin.GraphWriter which panics on its own.
//
//   - A developer changed a Cypher query, but missed some code that relied on that
//     query. This is indicated by the digitaltwin.GraphWriter returning
//     errPropertyNotFound or unexpectedPropertyTypeError, causing this function to
//     issue the panic directive.
func (e *Engine) Apply(ctx context.Context, compilation digitaltwin.Compilation) (err error) {
	ctx, span := tracer.Start(ctx, "Apply", trace.WithAttributes(
		attribute.String("neo4j.database", e.database),
	))
	defer span.End()
	logger := component.Logger(ctx).With("neo4j.database", e.database)

	// We open a new session for every query cycle to ensure transactional isolation
	// and to prevent any state carryover between different query executions.This
	// practice enhances robustness because any session-specific errors or resources
	// are contained and do not affect subsequent operations.
	s := e.driver.NewSession(ctx, neo4j.SessionConfig{
		DatabaseName: e.database,
		AccessMode:   neo4j.AccessModeWrite,
	})
	defer func() {
		if err := s.Close(ctx); err != nil {
			logger.Error("Failed to close session", "error", err, "mode", "write")
		}
	}()

	// We use a special mutex to exclusively either write or read.
	//
	// Here we lock for concurrent write-operations before initiating the
	// write-transaction to prevent other read operations from happening, which could
	// interfere with the consistent state of the graph as this transaction intends
	// to modify it.
	e.txMutex.WLock()
	defer e.txMutex.WUnlock()

	// We use write transactions because the neo4j SDK can provide transaction
	// management features such as retries, error handling, and deadlock resolution.
	_, err = s.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
		return nil, compilation(ctx, graphWriter{tx: tx, nodeTainter: &e.taintedNodes})
	})
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return err
	} else if errors.Is(err, errPropertyNotFound) || errors.As(err, &unexpectedPropertyTypeError{}) {
		component.Logger(ctx).Error("A Cypher query was modified without care", "error", err)
		panic(fmt.Errorf("seek developer attention: neo4j cypher query: %w", err))
	} else if err != nil {
		return fmt.Errorf("neo4j execute: %w", err)
	}
	return nil
}

// A errPropertyNotFound occurs when a property of Node/Edge is missing.
//
// When encountering this error, it most likely occurs when changing a Cypher
// query without modifying the surrounding code properly. Expect a panic
// eventually.
var errPropertyNotFound = errors.New("property not found")

// An unexpectedPropertyTypeError occurs when a property of Node/Edge has a
// runtime type that is different from the expected type. The error message
// contains the effective type of the property at runtime.
//
// When encountering this error, it most likely occurs when changing a Cypher
// query without modifying dependent code properly. Expect a panic eventually.
type unexpectedPropertyTypeError struct {
	Type reflect.Type // Effective type encountered at runtime.
}

func (e unexpectedPropertyTypeError) Error() string {
	return "unexpected property type: " + e.Type.String()
}
