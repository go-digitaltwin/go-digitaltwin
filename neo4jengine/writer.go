package neo4jengine

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/danielorbach/go-component"
	"github.com/go-digitaltwin/go-digitaltwin"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// A graphWriter implements [digitaltwin.GraphWriter] within a single neo4j transaction.
//
// It translates between digitaltwin.Value and RawNode, and relies on rawWriter
// to perform the actual modifications on the graph using carefully crafted
// Cypher queries.
type graphWriter struct {
	tx neo4j.ManagedTransaction
	// A nodeTainter defines how to taint nodes of graph components that were
	// modified during a compilation.
	nodeTainter interface {
		Taint(node ...RawNode)
	}
}

func (w graphWriter) AssertNode(ctx context.Context, node digitaltwin.Value) (err error) {
	x, err := FormatNode(node)
	if err != nil {
		return fmt.Errorf("format node: %w", err)
	}
	return w.assertNode(ctx, x)
}

func (w graphWriter) assertNode(ctx context.Context, node RawNode) (err error) {
	ca, err := node.ContentAddress.MarshalText()
	if err != nil {
		return fmt.Errorf("marshal content address: %w", err)
	}

	query := `
		MERGE (s:` + node.Label + ` {_contentAddress: $ca})
		ON CREATE SET s._created_at = datetime()
		SET s += $node_prop, s._last_modified = datetime()
		RETURN count(s) as nodes
	`
	result, err := w.tx.Run(ctx, query, map[string]any{
		"ca":        string(ca),
		"node_prop": node.Props,
	})
	if err != nil {
		return fmt.Errorf("run cypher: %w", err)
	}
	record, err := result.Single(ctx)
	if err != nil {
		return fmt.Errorf("query single result: %w", err)
	}

	nodes, err := getRecordProperty[int64](record, "nodes")
	if err != nil {
		return fmt.Errorf("get nodes: %w", err)
	}
	// A single digitaltwin.Value is represented by a single node in the underlying
	// graph. Asserting that value should create at most a single node (either it is
	// present in the graph, or it isn't). If the query creates/updates more than a
	// single node, it implies the underlying graph has lost its integrity, so we
	// cannot continue to operate on it.
	if nodes != 1 {
		panicWithCorruptedGraph(ctx, fmt.Sprintf("assert-node modified %v nodes instead of 1", nodes))
	}

	// We taint only the asserted node, as it is the sole node being created or
	// updated; no other nodes are affected by this operation.
	w.nodeTainter.Taint(node)

	return nil
}

func (w graphWriter) RetractNode(ctx context.Context, node digitaltwin.Value) (err error) {
	x, err := FormatNode(node)
	if err != nil {
		return fmt.Errorf("format node: %w", err)
	}
	return w.retractNode(ctx, x)
}

func (w graphWriter) retractNode(ctx context.Context, node RawNode) (err error) {
	ca, err := node.ContentAddress.MarshalText()
	if err != nil {
		return fmt.Errorf("marshal content address: %w", err)
	}

	query := `
		MATCH (n :` + node.Label + `{ _contentAddress: $ca })
		OPTIONAL MATCH (n)-[]-(taint)
		DETACH DELETE n
		RETURN count(DISTINCT n) AS nodes, COLLECT(DISTINCT taint) AS taints
	`
	result, err := w.tx.Run(ctx, query, map[string]any{
		"ca": string(ca),
	})
	if err != nil {
		return fmt.Errorf("run cypher: %w", err)
	}
	record, err := result.Single(ctx)
	if err != nil {
		return fmt.Errorf("query single result: %w", err)
	}

	nodes, err := getRecordProperty[int64](record, "nodes")
	if err != nil {
		return fmt.Errorf("get nodes: %w", err)
	}
	// A single digitaltwin.Value is represented by a single node in the underlying
	// graph. Retracting that value should delete at most a single node (either it is
	// present in the graph, or it isn't). If the query deletes more than a single
	// node, it implies the underlying graph has lost its integrity, so we cannot
	// continue to operate on it.
	if nodes != 1 && nodes != 0 {
		panicWithCorruptedGraph(ctx, fmt.Sprintf("retract-node modified %v nodes instead of 0/1", nodes))
	}

	// Lastly, mark touched nodes as tainted.
	taints, err := parseTaintedNodes(record)
	if err != nil {
		return fmt.Errorf("parse taints: %w", err)
	}
	// Only the retracted node is initially tainted as it has been directly changed;
	// however, nodes previously connected to it may also be contextually affected.
	w.nodeTainter.Taint(node)
	// Connected nodes are tainted because the structure of their relationships has
	// now changed due to the node retraction.
	w.nodeTainter.Taint(taints...)

	return nil
}

func (w graphWriter) AssertEdge(ctx context.Context, from, to digitaltwin.Value) (err error) {
	src, err := FormatNode(from)
	if err != nil {
		return fmt.Errorf("format 'from' node: %w", err)
	}
	dst, err := FormatNode(to)
	if err != nil {
		return fmt.Errorf("format 'to' node: %w", err)
	}
	return w.assertEdge(ctx, src, dst)
}

func (w graphWriter) assertEdge(ctx context.Context, from, to RawNode) (err error) {
	fromContentAddress, err := from.ContentAddress.MarshalText()
	if err != nil {
		return fmt.Errorf("marshal content address: %w", err)
	}

	toContentAddress, err := to.ContentAddress.MarshalText()
	if err != nil {
		return fmt.Errorf("marshal content address: %w", err)
	}

	query := `
		MERGE (s:` + from.Label + ` {_contentAddress: $from})
		ON CREATE SET s._created_at = datetime()
		SET s += $src, s._last_modified = datetime()

		MERGE (d:` + to.Label + ` {_contentAddress: $to})
		ON CREATE SET d._created_at = datetime()
		SET d += $dst, d._last_modified = datetime()

		MERGE (s)-[e:CONNECTS]->(d)
		ON CREATE SET e._created_at = datetime()
		SET e._last_modified = datetime()

		RETURN count(e) as edges
	`
	result, err := w.tx.Run(ctx, query, map[string]any{
		"from": string(fromContentAddress),
		"src":  from.Props,
		"to":   string(toContentAddress),
		"dst":  to.Props,
	})
	if err != nil {
		return fmt.Errorf("run cypher: %w", err)
	}
	record, err := result.Single(ctx)
	if err != nil {
		return fmt.Errorf("query single result: %w", err)
	}

	edges, err := getRecordProperty[int64](record, "edges")
	if err != nil {
		return fmt.Errorf("get edges: %w", err)
	}
	// A single digitaltwin.Value is represented by a single node in the underlying
	// graph. Asserting an edge between two digitaltwin.Value ensures the existence
	// of an edge (either it is present in the graph, or it isn't). If the query
	// creates more than a single edge, it implies the underlying graph has lost its
	// integrity, so we cannot continue to operate on it.
	if edges != 1 {
		panicWithCorruptedGraph(ctx, fmt.Sprintf("assert-edge modified %v edges instead of 1", edges))
	}

	// We taint the source and target nodes as they are directly involved in the
	// creation or validation of an edge, without impacting any other nodes.
	w.nodeTainter.Taint(from, to)

	return nil
}

func (w graphWriter) RetractEdges(ctx context.Context, node digitaltwin.Value, kind reflect.Type) (n int, err error) {
	x, err := FormatNode(node)
	if err != nil {
		return 0, fmt.Errorf("format node: %w", err)
	}
	label, ok := LabelOf(kind)
	if !ok {
		return 0, errors.New("unregistered node kind")
	}
	return w.retractEdges(ctx, x, label)
}

func (w graphWriter) retractEdges(ctx context.Context, node RawNode, label string) (n int, err error) {
	ca, err := node.ContentAddress.MarshalText()
	if err != nil {
		return 0, fmt.Errorf("marshal content address: %w", err)
	}

	query := `
		Match (:` + node.Label + `{_contentAddress: $from})-[e]-(taint:` + label + `)
		DELETE e
		RETURN count(e) as edges, COLLECT(DISTINCT taint) AS taints
	`
	result, err := w.tx.Run(ctx, query, map[string]interface{}{
		"from": string(ca),
	})
	if err != nil {
		return 0, fmt.Errorf("run cypher: %w", err)
	}
	record, err := result.Single(ctx)
	if err != nil {
		return 0, fmt.Errorf("query single result: %w", err)
	}

	edges, err := getRecordProperty[int64](record, "edges")
	if err != nil {
		return 0, fmt.Errorf("get edges: %w", err)
	}

	// Lastly, mark touched nodes as tainted.
	taints, err := parseTaintedNodes(record)
	if err != nil {
		return 0, fmt.Errorf("parse taints: %w", err)
	}
	// The originating node of the retracted edge is tainted because it loses a
	// connection; yet this operation doesn't affect its other relationships.
	w.nodeTainter.Taint(node)
	// Connected nodes are also tainted as their direct links to the originating node
	// have been removed, altering their adjacency.
	w.nodeTainter.Taint(taints...)

	return int(edges), nil
}

// We modify the underlying neo4j graph database in a way that prompts us when
// the graph violates some of our basic constraints.
//
// When we suspect the graph has lost its integrity, we may no longer operate on
// it. In which case, we must immediately stop all operations. This is achieved
// with a panic preceded by telemetry signals (traces, metrics, and logs) to
// bring the situation to our immediate attention.
func panicWithCorruptedGraph(ctx context.Context, reason string) {
	component.Logger(ctx).ErrorContext(ctx, "Encountered corrupted neo4j graph that violates digital-twin axioms", "error", reason)
	trace.SpanFromContext(ctx).SetStatus(codes.Error, reason)
	// TODO(@marombracha): let's measure the frequency of this fatality.
	panic(fmt.Errorf("neo4j graph violates digital-twin axioms: %v", reason))
}

// Call this function to extract the tainted nodes (as defined by the Cypher
// query in the individual graphWriter methods) that change during a graph
// modification.
func parseTaintedNodes(record *neo4j.Record) (taints []RawNode, err error) {
	nodes, err := getRecordProperty[[]interface{}](record, "taints")
	if err != nil {
		return nil, fmt.Errorf("get taints: %w", err)
	}
	taints = make([]RawNode, len(nodes))
	for i, n := range nodes {
		node, ok := n.(neo4j.Node)
		if !ok {
			return nil, unexpectedPropertyTypeError{Type: reflect.TypeOf(n)}
		}
		taint, err := newRawNode(node)
		if err != nil {
			return taints, fmt.Errorf("parse raw node: %w", err)
		}
		taints[i] = taint
	}
	return taints, nil
}
