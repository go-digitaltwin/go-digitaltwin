package neo4jengine

import (
	"context"
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"

	"github.com/danielorbach/go-component"
)

// RewriteNodesContentAddress changes the content-address stored for all nodes to
// align with the latest representation.
//
// Previously, the RawNode.ContentAddress field had type 'string'. At that time,
// the code called NodeHash.String() to acquire the appropriate string value to
// store in the database.
//
// Nowadays, we use a digitaltwin.NodeHash value directly. Nonetheless, we want
// this version of the codebase to remain compatible with the data persisted in
// the graph by older versions.
//
// Although running this rewrite on the same graph is harmless (i.e. idempotent
// action), it is tailored specifically to assist upgrading digital-twins from
// release v0.2 to v0.3 (which is the planned version for the upcoming refactor).
//
// TODO: remove this backwards compatibility scaffolding once all deployed environments are upgraded.
func RewriteNodesContentAddress(ctx context.Context, d neo4j.DriverWithContext, name string) error {
	logger := component.Logger(ctx).With("neo4j.database", name)

	s := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: name, AccessMode: neo4j.AccessModeWrite})
	defer func() {
		if err := s.Close(ctx); err != nil {
			logger.Error("Failed to close neo4j session", "error", err)
		}
	}()

	// NodeHash.String() wraps NodeHash.MarshalText() as "node(...)", so we retain
	// compatibility by removing the surrounding parenthesis.
	result, err := s.Run(ctx, `
		MATCH (n)
		WHERE n._contentAddress STARTS WITH 'node(' AND n._contentAddress ENDS WITH ')'
		SET n._contentAddress = substring(
		  n._contentAddress,
		  5,
		  size(n._contentAddress) - 6
		)
		RETURN count(n) as count
	`, nil)
	if err != nil {
		return err
	}

	record, err := result.Single(ctx)
	if err != nil {
		return fmt.Errorf("query single result: %w", err)
	}
	affected, err := getRecordProperty[int64](record, "count")
	if err != nil {
		return fmt.Errorf("get number of affected nodes: %w", err)
	}

	logger.Info("All content-addresses in the graph were successfully rewritten", "count", affected)
	return nil
}
