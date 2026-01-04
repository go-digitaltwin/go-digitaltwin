package neo4jengine

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"

	"github.com/go-digitaltwin/go-digitaltwin"
	"github.com/go-digitaltwin/go-digitaltwin/internal/dbtest"
)

func TestBootstrapDatabase(t *testing.T) {
	d := dbtest.SetupNeo4j(t)

	// register some labels to make sure constraints are executed
	{
		type someNode struct {
			digitaltwin.InformationElement
		}
		Register(someNode{})
		type someOtherNode struct {
			digitaltwin.InformationElement
		}
		Register(someOtherNode{})
	}

	var tests = []struct {
		name     string
		database string
	}{
		{name: "Alphanumeric", database: "Aa1"},
		{name: "WithDash", database: "a-1"},
		{name: "WithDot", database: "a.1"},
		{name: "UUID", database: "a1b2c3d4-e5f6-4a1b-9c2d-3e4f5a6b7c8d"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			err := BootstrapDatabase(ctx, d, tt.database)
			if err != nil {
				t.Fatalf("BootstrapDatabase() error = %v", err)
			}

			// execute a query to make sure the constraints are in place and the database is usable
			session := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: tt.database})
			defer func() {
				if err := session.Close(ctx); err != nil {
					t.Fatal("Failed to close session:", err)
				}
			}()

			// filter NODE_KEY constraints because the database contains other implicit
			// constraints that we don't care about for each label
			result, err := session.Run(ctx, "SHOW CONSTRAINTS WHERE type = 'NODE_KEY'", nil)
			if err != nil {
				t.Fatal("Failed to list constraints:", err)
			}

			var found1, found2 bool
			for result.Next(ctx) {
				t.Log(formatRecord(result.Record()))

				// see https://neo4j.com/docs/cypher-manual/current/constraints/examples/#constraints-examples-list-constraint
				labels, ok := result.Record().Get("labelsOrTypes")
				if !ok {
					t.Fatal("Constraints table contains no labels column")
				}
				for _, label := range labels.([]interface{}) {
					if label == "someNode" {
						found1 = true
					}
					if label == "someOtherNode" {
						found2 = true
					}
				}
			}
			if err := result.Err(); err != nil {
				t.Fatal("Failed to list constraints:", err)
			}

			if !found1 {
				t.Error("Constraint for label someNode not found")
			}
			if !found2 {
				t.Error("Constraint for label someOtherNode not found")
			}
		})
	}

	t.Run("InvalidName", func(t *testing.T) {
		var tests = []struct {
			name      string
			database  string
			wantPanic bool
		}{
			{name: "Empty", wantPanic: true},
			{name: "Reserved(neo4j)", database: "neo4j", wantPanic: true},
			{name: "Reserved(system)", database: "systemReserved", wantPanic: true},
			{name: "Reserved(underscore)", database: "_NotSystem", wantPanic: true},
			{name: "TooShort", database: "aa"},
			{name: "TooLong", database: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa64"},
			{name: "IllegalChars(underscore)", database: "a_1"},
			{name: "IllegalChars(slash)", database: "a/1"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				defer func() {
					if r := recover(); (r != nil) != tt.wantPanic {
						t.Errorf("BootstrapDatabase() panic = %v, wantPanic %v", r, tt.wantPanic)
					}
				}()

				err := BootstrapDatabase(context.Background(), d, tt.database)
				if err == nil {
					t.Errorf("BootstrapDatabase() succeeded, want error")
				}
			})
		}
	})
}

func formatRecord(r *neo4j.Record) string {
	var fields []string
	for i, key := range r.Keys {
		fields = append(fields, fmt.Sprintf("%s: %v", key, r.Values[i]))
	}
	return strings.Join(fields, ", ")
}
