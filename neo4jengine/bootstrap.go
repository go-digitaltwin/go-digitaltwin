package neo4jengine

import (
	"context"
	"fmt"
	"strings"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// BootstrapDatabase creates the necessary constraints and indexes for the
// database to be suitable for use by a digital twin.
//
// Index by content-address for optimised lookups, and constraint uniqueness by
// content-address to prevent duplicate nodes (caused by concurrent MERGEs).
//
// To execute queries against the created database, open a session with the
// database name as the default database. For example:
//
//	s := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: name})
//	defer func() { _ = s.Close(ctx) }()
//	... use s ...
//
// This function is idempotent.
func BootstrapDatabase(ctx context.Context, d neo4j.DriverWithContext, name string) error {
	if err := createDatabase(ctx, d, name); err != nil {
		return fmt.Errorf("create database: %w", err)
	}

	s := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: name})
	defer func() { _ = s.Close(ctx) }()

	// create constraints and indexes for all known labels
	_, err := s.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
		for _, l := range KnownLabels() {
			// we use key constraint instead of uniqueness constraint because we can
			// (it is only available in the enterprise edition).
			_, err := s.Run(ctx, `
				CREATE CONSTRAINT IF NOT EXISTS
				FOR (n:`+l+`)
				REQUIRE n._contentAddress IS NODE KEY
			`, nil)
			if err != nil {
				return nil, fmt.Errorf("key constraint: label %v: %w", l, err)
			}
		}
		return nil, nil
	})
	if err != nil {
		return fmt.Errorf("create constraints: %w", err)
	}
	return s.Close(ctx)
}

func createDatabase(ctx context.Context, d neo4j.DriverWithContext, name string) error {
	if name == "" {
		panic("neo4jengine: database name must not be empty")
	}
	if name == "neo4j" {
		panic("neo4jengine: database name must not be neo4j: reserved for system database")
	}
	if strings.HasPrefix(name, "system") || strings.HasPrefix(name, "_") {
		panic("neo4jengine: Names that begin with an underscore and with the prefix system are reserved for internal use")
	}

	s := d.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer func() { _ = s.Close(ctx) }()

	// create a new database if it does not exist
	_, err := s.Run(ctx, `
			CREATE DATABASE $name IF NOT EXISTS
		`, map[string]interface{}{
		"name": name,
	})
	return err
}
