package neo4jengine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"

	"github.com/danielorbach/go-component"
	"github.com/go-digitaltwin/go-digitaltwin"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"go.opentelemetry.io/otel/attribute"
)

// A snapshot stores the current assembly-graphs in a digital-twin system. It is
// mostly used to compute the difference between two snapshots using the
// WhatChanged method.
type snapshot map[digitaltwin.ComponentID]digitaltwin.ComponentHash

// This function uses the given neo4j connection to iterate over the entire graph
// (specified by the given database name) while identifying disjoint graph
// components.
//
// The returned snapshot records all the identified disjoint graph components.
func captureSnapshot(ctx context.Context, d neo4j.DriverWithContext, database string) (snapshot, error) {
	logger := component.Logger(ctx).With("neo4j.database", database)

	s := d.NewSession(ctx, neo4j.SessionConfig{
		DatabaseName: database,
		AccessMode:   neo4j.AccessModeRead,
	})
	defer func() {
		if err := s.Close(ctx); err != nil {
			logger.Error("Failed to close engine's read session", "error", err)
		}
	}()

	ss := make(snapshot)
	// First, get a cursor into the entire graph.
	result, err := fetchAssemblies(ctx, s)
	if err != nil {
		return ss, fmt.Errorf("fetch assemblies: %w", err)
	}
	// Remember to consume (discards all remaining records) before exiting. Failing
	// to do so may leak resources, we're not sure.
	defer func() {
		_, err := result.Consume(ctx) // Currently, we do not know what to do with the summary, so ignore it.
		if err != nil {
			logger.Error("Failed to drain a neo4j connection", "error", err)
		}
	}()

	for result.Next(ctx) {
		a, err := safelyParseAssembly(ctx, result.Record())
		if err != nil {
			return ss, fmt.Errorf("parse assembly: %w", err)
		}
		ss[a.AssemblyID()] = a.AssemblyHash()
	}
	// Neo4j's result cursor is exhausted by now. We check its Err method to get the
	// error that caused the iteration to stop, if any.
	if err := result.Err(); err != nil {
		return ss, fmt.Errorf("iterate assemblies: %w", err)
	}
	return ss, nil
}

// GraphHash calculates and returns a consolidated hash representing the entire
// state of the snapshot by hashing its components. Using this, one can quickly
// determine if two Snapshots are identical or if any changes have occurred
// between them.
func (s snapshot) GraphHash() digitaltwin.ForestHash {
	return digitaltwin.HashComponents(s)
}

// ContainsAssembly determines whether the snapshot contains an assembly with the
// same ID and an unchanged hash, indicating that the assembly has not been
// altered since the snapshot was taken. It returns true if both the assembly
// exists and the hash matches.
func (s snapshot) ContainsAssembly(a digitaltwin.AssemblyRef) bool {
	hash, exists := s[a.AssemblyID()]
	return exists && hash == a.AssemblyHash()
}

// The fetchAssemblies function returns a list of assemblies from the Neo4j graph
// associated with the given session.
//
// Every record in the query results contains:
//
//   - A "root" property marking the root node of the assembly.
//
//   - A list of neighbour "tuples", such that every tuple has a "from" and a "to"
//     Node.
//
// We assume the following statements are true:
//   - Assembly is DAG.
//   - Assembly edges are not wighted (i.e. no properties).
//   - Assembly has only one root.
//
// If any of those assumptions are false, then we cannot guarantee the behaviour
// of the query.
func fetchAssemblies(ctx context.Context, s neo4j.SessionWithContext) (neo4j.ResultWithContext, error) {
	query := `
		CALL {
			// find roots
			MATCH (root) WHERE NOT EXISTS {()-[]->(root)}

			// find all paths possibly few paths from same root!!! be aware.
			// only MATCH roots of path in length of 8 or less.
			MATCH (root)-[*0..5]->(path_node)-[]->(adjacent_path_node)

			// group all tuples by root, tuples are unique since they are added
			WITH root, COLLECT({from: path_node, to: adjacent_path_node}) AS tuples
			RETURN root, tuples
			Union
			MATCH (root) WHERE NOT EXISTS {()-[]->(root)} AND NOT EXISTS {()<-[]-(root)}
			RETURN root, [{from: null, to:null}] AS tuples
		}
		RETURN root, tuples
	`
	result, err := s.Run(ctx, query, nil)
	if err != nil {
		return nil, fmt.Errorf("run: %w", err)
	}
	return result, nil
}

// Call fetchPartialAssemblies to fetch (from Neo4j graph associated with the
// given session) the assemblies that were touched, as marked by the given
// slice of tainted nodes.
//
// Every record in the query results contains:
//
//   - A "root" property marking the root node of the assembly.
//
//   - A list of neighbour "tuples", such that every tuple has a "from" and a "to"
//     Node.
//
// We assume the following statements are true:
//   - Assembly is DAG.
//   - Assembly edges are not wighted (i.e. no properties).
//   - Assembly has only one root.
//
// If any of those assumptions are false, then we cannot guarantee the behaviour
// of the query.
func fetchPartialAssemblies(ctx context.Context, s neo4j.SessionWithContext, taints []RawNode) (assemblies []digitaltwin.Assembly, err error) {
	ctx, span := tracer.Start(ctx, "fetchPartialAssemblies")
	defer span.End()

	work := func(tx neo4j.ManagedTransaction) (interface{}, error) {
		// We use a map to track disjoint graph components and their respective hashes,
		// to ensure consistency during graph read iterations, since we do not fully
		// understand Neo4j's isolation levels.
		//
		// We think that concurrent transactions might affect our data accuracy because
		// we've noticed that modifications made in one write-transaction spill over into
		// an already running read-transaction.
		//
		// As we read the graph during a single transaction, we must guarantee identical
		// results for repeated reads of the same disjoint graph components. Any
		// discrepancy in results would invalidate our ability to compare graph states,
		// so we choose to immediately abort the operation and panic.
		seen := make(map[digitaltwin.ComponentID]digitaltwin.ComponentHash)

		// We are only collecting assemblies containing nodes we have already tainted.
		for _, taint := range taints {
			ca, err := taint.ContentAddress.MarshalText()
			if err != nil {
				return nil, fmt.Errorf("marshal content address: %w", err)
			}
			query := `
				CALL{
					MATCH (root)-[*]->(target:` + taint.Label + `{_contentAddress: $ca})
					WHERE NOT ()-->(root) // No incoming of any type to root
					WITH root
					MATCH (root)-[*0..5]->(path_node)-[]->(adjacent_path_node)
					WITH root, COLLECT({from: path_node, to: adjacent_path_node}) AS tuples
					RETURN root, tuples

					UNION

					MATCH (root:` + taint.Label + `{_contentAddress: $ca})
					WHERE NOT ()-->(root) AND NOT ()<--(root)
					RETURN root, [{from: null, to: null}] AS tuples
				}
				return root, tuples
			`
			result, err := tx.Run(ctx, query, map[string]any{"ca": string(ca)})
			if err != nil {
				return nil, fmt.Errorf("run: %w", err)
			}
			for result.Next(ctx) {
				a, err := safelyParseAssembly(ctx, result.Record())
				if err != nil {
					return nil, fmt.Errorf("parse assembly: %w", err)
				}

				id := a.AssemblyID()
				h, exists := seen[id]
				// If it's the first time encountering this assembly, mark it.
				if !exists {
					seen[id] = a.AssemblyHash()
					assemblies = append(assemblies, a)
				}
				// If the current assembly has been previously marked as seen, we check whether
				// the stored hash matches the already seen hash.
				//
				// A mismatch indicates an inconsistency in the transaction's isolation, so we
				// inevitably panic.
				if exists && h != a.AssemblyHash() {
					span.SetAttributes(
						attribute.Stringer("assembly.id", id),
						attribute.Stringer("assembly.hash", a.AssemblyHash()),
						attribute.Stringer("seen.hash", h),
					)
					component.Logger(ctx).Error(
						"An assembly was modified while in a read transaction, this should not happen",
						slog.String("assembly.id", id.String()),
						slog.String("assembly.hash", a.AssemblyHash().String()),
						slog.String("assembly.seenHash", h.String()),
					)
					panic(fmt.Errorf("seek developer attention: a neo4j transaction isolation was violated"))
				}
			}
			// Neo4j's result cursor is exhausted by now. We check its Err method to get the
			// error that caused the iteration to stop, if any.
			if err := result.Err(); err != nil {
				return nil, fmt.Errorf("iterate assembly: %w", err)
			}
		}
		return nil, nil
	}

	// The work function above appends directly into the returned assemblies
	// variable.
	_, err = s.ExecuteRead(ctx, work)
	if err != nil {
		return nil, fmt.Errorf("execute read: %w", err)
	}
	return assemblies, nil
}

// Computes the [digitaltwin.ComponentID] of an assembly containing only the given RawNode (as its root).
//
// We collect those digitaltwin.ComponentID, to compute diff from the old full
// snapshot to the new partial one. If the component ID was in the old snapshot
// but isn't in the newer partial snapshot, we can draw that it was removed.
func componentID(taint RawNode) (id digitaltwin.ComponentID, err error) {
	v, err := ParseNode(taint)
	if err != nil {
		return id, fmt.Errorf("parse taint: %w", err)
	}
	var b digitaltwin.AssemblyBuilder
	b.Roots(v)
	return b.Assemble().AssemblyID(), nil
}

// Call this function to parse a record representing an assembly (as constructed
// the Cypher query defined by fetchAssemblies) with a possible panic due to
// developer errors.
//
// Developer errors happen when a developer had changed some code that depends on
// the specifics of the Cypher query, but missed some bits.
func safelyParseAssembly(ctx context.Context, record *neo4j.Record) (assembly digitaltwin.Assembly, err error) {
	assembly, err = parseAssembly(record)
	if errors.Is(err, errPropertyNotFound) || errors.As(err, &unexpectedPropertyTypeError{}) {
		component.Logger(ctx).Error("A Cypher query was modified without care", "error", err)
		panic(fmt.Errorf("seek developer attention: neo4j cypher query: %w", err))
	}
	return
}

// Call safelyParseAssembly instead of calling this function directly. Following
// this directive ensures the same developer errors are panicked regardless of
// the code-path that encounters them.
func parseAssembly(record *neo4j.Record) (digitaltwin.Assembly, error) {
	r, err := getRecordProperty[neo4j.Node](record, "root")
	if err != nil {
		return nil, fmt.Errorf("get root: %w", err)
	}
	root, err := parseNeo4jNode(r)
	if err != nil {
		return nil, fmt.Errorf("root: %w", err)
	}

	var builder digitaltwin.AssemblyBuilder
	builder.Roots(root)
	if err := parseNeighbours(record, &builder); err != nil {
		return nil, fmt.Errorf("parse neighbours: %w", err)
	}
	return builder.Assemble(), nil
}

// This function is here to make parsing neo4j.Node into digitaltwin.Value more
// readable at the call-site.
func parseNeo4jNode(node neo4j.Node) (digitaltwin.Value, error) {
	raw, err := newRawNode(node)
	if err != nil {
		return nil, fmt.Errorf("construct raw node: %w", err)
	}
	v, err := ParseNode(raw)
	if err != nil {
		return nil, fmt.Errorf("parse raw node: %w", err)
	}
	return v, nil
}

func parseNeighbours(record *neo4j.Record, builder *digitaltwin.AssemblyBuilder) error {
	tuples, err := getRecordProperty[[]interface{}](record, "tuples")
	if err != nil {
		return fmt.Errorf("get tuples :%w", err)
	}

	for i, tuple := range tuples {
		edge, ok := tuple.(map[string]interface{})
		if !ok {
			return fmt.Errorf("neighbour tuple #%v: %w", i, unexpectedPropertyTypeError{Type: reflect.TypeOf(tuple)})
		}
		// If the query encounters a floating node in the graph, it outputs its tuple
		// list as a single entry containing an edge [from: null, to: null]. This
		// signifies that the node is an isolated root. The following check ensures we
		// only process valid edges by skipping such standalone nodes as they already
		// build on parseAssembly.
		if edge["from"] == nil && edge["to"] == nil {
			continue
		}

		err := parseNeighbour(edge, builder)
		if err != nil {
			return fmt.Errorf("neighbour #%v: %w", i, err)
		}
	}

	return nil
}

// Call parseNeighbour with a single "tuple" from the "tuples" slice, as
// collected by the Cypher query defined at fetchAssemblies.
func parseNeighbour(edge map[string]interface{}, builder *digitaltwin.AssemblyBuilder) error {
	// Construct the source node of the edge.
	from, ok := edge["from"]
	if !ok {
		return fmt.Errorf("get from: %w", errPropertyNotFound)
	}
	fromNode, ok := from.(neo4j.Node)
	if !ok {
		return fmt.Errorf("get from: %w", unexpectedPropertyTypeError{Type: reflect.TypeOf(from)})
	}
	source, err := parseNeo4jNode(fromNode)
	if err != nil {
		return fmt.Errorf("source node: %w", err)
	}

	// Construct the target node of the edge.
	to, ok := edge["to"]
	if !ok {
		return fmt.Errorf("get to: %w", errPropertyNotFound)
	}
	toNode, ok := to.(neo4j.Node)
	if !ok {
		return fmt.Errorf("get to: %w", unexpectedPropertyTypeError{Type: reflect.TypeOf(to)})
	}
	target, err := parseNeo4jNode(toNode)
	if err != nil {
		return fmt.Errorf("target node: %w", err)
	}

	// Connect the two nodes of the edge.
	builder.Connect(source, target)
	return nil
}

// Diff calculates the difference between two snapshots, each containing the
// disjoint graph components of complete digital-twin graphs (usually the same
// graph at different points in time).
//
// Diff returns which disjoint graph components were created, updated, or
// removed, while those that did not change are not returned.
func (s snapshot) Diff(newer snapshot) (created, updated, removed []digitaltwin.ComponentID) {
	// Assemblies that appear in the newer snapshot could be created, updated, or
	// unchanged.
	for id, newHash := range newer {
		if oldHash, ok := s[id]; !ok {
			created = append(created, id)
		} else if oldHash != newHash {
			updated = append(updated, id)
		} // else: no change
	}

	// Assemblies that appear in the older snapshot but not in the newer snapshot
	// have been removed.
	for id := range s {
		if _, ok := newer[id]; !ok {
			removed = append(removed, id)
		}
	}

	return created, updated, removed
}

// PartialDiff calculate the difference between this full snapshot (containing
// all disjoint graph components of a digital-twin graph) and a partial snapshot
// containing some disjoint graph components.
//
// PartialDiff returns which disjoint graph components were created, updated, or
// removed, while those that did not change are not returned.
//
// PartialDiff compares against a partial snapshot, so its knowledge of the
// entire graph is limited by the assemblies contained in that snapshot. That is,
// the function cannot conclude if an assembly that was part of this snapshot is
// removed with certainty, solely based on the given partial snapshot.
//
// The dirtyRoots contains the component-ids of all nodes that were touched
// during the write operations leading to the given partial snapshot. With this
// knowledge, we can know for sure which assemblies were removed from this
// snapshot. Read the iteration over the dirtyRoots with care to see this in
// action.
//
// Hint, consider every dirtyRoot component to be a component build of a single
// node that was tainted before calling this function.
func (s snapshot) PartialDiff(partial snapshot, dirtyRoots []digitaltwin.ComponentID) (created, updated, removed []digitaltwin.ComponentID) {
	// Assemblies that appear in the newer snapshot could be created, updated, or
	// unchanged.
	for id, newHash := range partial {
		if oldHash, ok := s[id]; !ok {
			created = append(created, id)
		} else if oldHash != newHash {
			updated = append(updated, id)
		}
	}

	for _, id := range dirtyRoots {
		_, wasRoot := s[id]       // Check if the tainted component was in the old snapshot.
		_, nowRoot := partial[id] // Check if the tainted component is not present in the newer partial snapshot.
		// If the tainted node was a root in the old snapshot but isn't in the newer
		// partial snapshot, we assume that the component has been removed.
		//
		// This assumption is legitimate since dirtyRoots contain components (built of a
		// single node) that we know were affected by the write operations leading to the
		// creation of the partial snapshot.
		if !nowRoot && wasRoot {
			removed = append(removed, id)
		}
	}
	return created, updated, removed
}

// Update modifies the current snapshot based on the changes observed in the
// digital twin's graph. It processes information about which components have
// been created, updated, or removed, and adjusts the snapshot to reflect the new
// state of the entire graph.
//
// This ensures that the snapshot stays current and accurately represents the
// graph's state after the changes have occurred.
//
// It is designed to work hand in hand with PartialDiff.
func (s snapshot) Update(changes digitaltwin.GraphChanged) {
	for _, created := range changes.Created {
		s[created.AssemblyID()] = created.AssemblyHash()
	}
	for _, updated := range changes.Updated {
		s[updated.AssemblyID()] = updated.AssemblyHash()
	}
	for _, removed := range changes.Removed {
		delete(s, removed.AssemblyID())
	}
}

// The recordProperty interface defines generic constraints for supported values
// by getRecordProperty.
//
// These type constraints protect against unsupported neo4j types like int,
// uint32, etc.
//
// This is a subset of all types supported by the neo4j package because listing
// all of them would be troublesome. When a new type is necessary, developers can
// simply add it to the list here.
type recordProperty interface {
	int64 | string | neo4j.Node | []interface{}
}

func getRecordProperty[T recordProperty](record *neo4j.Record, key string) (value T, err error) {
	prop, exists := record.Get(key)
	if !exists {
		return value, errPropertyNotFound
	}
	v, ok := prop.(T)
	if !ok {
		return value, unexpectedPropertyTypeError{Type: reflect.TypeOf(prop)}
	}
	return v, nil
}
