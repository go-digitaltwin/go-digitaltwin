package digitaltwin

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log/slog"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/danielorbach/go-component"
)

func TestComponentChangedGobMarshalling(t *testing.T) {
	tests := []struct {
		Name  string
		Value ComponentChanged
	}{
		{
			Name: "AssemblyCreated",
			Value: ComponentChanged{
				Assembly:  AssemblyCreated{Assembly: newAssembly("1")},
				GraphHash: ForestHash{1},
			},
		},
		{
			Name: "AssemblyUpdated",
			Value: ComponentChanged{
				Assembly:  AssemblyUpdated{Assembly: newAssembly("2")},
				GraphHash: ForestHash{1},
			},
		},
		{
			Name: "AssemblyRemoved",
			Value: ComponentChanged{
				Assembly: AssemblyRemoved{
					ID:   ComponentID{1},
					Hash: ComponentHash{0xaa},
				},
				GraphHash: ForestHash{1},
			},
		},
	}

	for _, tt := range tests {
		var p bytes.Buffer
		enc := gob.NewEncoder(&p)
		if err := enc.Encode(tt.Value); err != nil {
			t.Errorf("Encode(%s): %s", tt.Name, err)
			continue
		}

		var reconstructed ComponentChanged
		dec := gob.NewDecoder(&p)
		if err := dec.Decode(&reconstructed); err != nil {
			t.Errorf("Encode(%s): %s", tt.Name, err)
			continue
		}

		if diff := cmp.Diff(tt.Value, reconstructed); diff != "" {
			t.Errorf("Reconstructed %s value differs: %s", tt.Name, diff)
			continue
		}
	}
}

// ExampleDisassembler an example [component.Descriptor] for a digital-twin
// disassembler with an example bootstrap function.
func ExampleNewDisassembler() {
	graphChangedAspect := "asset-twin.graph-changed"
	componentChangedAspect := "asset-twin.component-changed"

	d := &component.Descriptor{
		Name: "assettwin-disassembler",
		Doc:  "....",
		Bootstrap: func(l *component.L, target component.Linker, options any) error {
			logger := component.Logger(l.Context())

			logger.Debug("Opening interest subscription...", slog.String("topic-name", graphChangedAspect))
			graphChanges, err := target.LinkInterest(l.GraceContext(), graphChangedAspect)
			if err != nil {
				return fmt.Errorf("open interest %q: %w", graphChangedAspect, err)
			}
			l.CleanupBackground(graphChanges.Shutdown)
			logger.Info("Interest subscription opened successfully")

			logger.Debug("Opening aspect topic...", slog.String("topic-name", componentChangedAspect))
			componentsChanges, err := target.LinkAspect(l.GraceContext(), componentChangedAspect)
			if err != nil {
				return fmt.Errorf("open aspect %q: %w", componentChangedAspect, err)
			}
			l.CleanupContext(componentsChanges.Shutdown)
			logger.Info("Aspect topic opened successfully")

			l.Fork("disassembler", NewDisassembler("assettwin", graphChanges, componentsChanges))

			return nil
		},
		Aspects:   []string{componentChangedAspect},
		Interests: []string{graphChangedAspect},
	}

	fmt.Print(d)
}
