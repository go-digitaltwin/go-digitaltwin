package neo4jengine

import (
	"context"
	"testing"

	"github.com/go-digitaltwin/go-digitaltwin/internal/dbtest"
	"github.com/go-digitaltwin/go-digitaltwin/enginetest"
)

func init() {
	Register(enginetest.NodeA{})
	Register(enginetest.NodeB{})
	Register(enginetest.NodeC{})
	Register(enginetest.NodeD{})
}

func TestEngine(t *testing.T) {
	driver := dbtest.SetupNeo4j(t)
	engine, err := NewEngine(context.Background(), driver, "neo4j")
	if err != nil {
		t.Fatal(err)
	}
	enginetest.Run(t, engine, engine)
}
