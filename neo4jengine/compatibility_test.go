package neo4jengine

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"

	"github.com/go-digitaltwin/go-digitaltwin/internal/dbtest"
)

func TestRewriteNodesContentAddress(t *testing.T) {
	ctx := context.Background()
	d := dbtest.SetupNeo4j(t)
	s := d.NewSession(ctx, neo4j.SessionConfig{
		DatabaseName: "neo4j",
		AccessMode:   neo4j.AccessModeWrite,
	})
	defer func() {
		if err := s.Close(ctx); err != nil {
			t.Errorf("Failed to close neo4j session: %v", err)
		}
	}()

	// We are creating a test graph that includes all the cases for testing rewrite.
	_, err := s.Run(ctx, `
		CREATE ({_contentAddress: "node(rewritten)"})
		CREATE (n {_contentAddress: "unmodified"})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to seed graph with testdata: %v", err)
	}

	err = RewriteNodesContentAddress(ctx, d, "neo4j")
	if err != nil {
		t.Errorf("RewriteNodesContentAddress() = %v", err)
	}

	// Retrieve all content addresses from nodes in the test Neo4j graph after the
	// rewrite.
	cas := contentAddresses(t, s)
	// We expect the rewrite to only modify the nodes in the old format, while
	// leaving the all other nodes, making the function harmless to execute multiple
	// times (e.g. every bootstrap).
	golden := []string{"rewritten", "unmodified"}

	opts := []cmp.Option{cmpopts.SortSlices(func(l, r string) bool { return l < r })}
	if diff := cmp.Diff(golden, cas, opts...); diff != "" {
		t.Errorf("RewriteNodesContentAddress() mismatch (-want +got)\n%v", diff)
	}
}

func contentAddresses(t *testing.T, s neo4j.SessionWithContext) []string {
	t.Helper()

	result, err := s.Run(context.Background(), `MATCH (n) RETURN n._contentAddress as contentAddress`, nil)
	if err != nil {
		t.Fatalf("Failed to query all nodes: %v", err)
	}

	records, err := result.Collect(context.Background())
	if err != nil {
		t.Errorf("Failed to collect query results: %v", err)
	}

	cas := make([]string, len(records))
	for i, record := range records {
		contentAddress, err := getRecordProperty[string](record, "contentAddress")
		if err != nil {
			t.Errorf("Failed to get #%v contentAddress property: %v", i, err)
		}
		cas[i] = contentAddress
	}
	return cas
}
