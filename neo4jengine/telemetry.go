package neo4jengine

import (
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

var tracer = otel.Tracer("github.com/go-digitaltwin/go-digitaltwin/neo4jengine")
var meter = otel.Meter("github.com/go-digitaltwin/go-digitaltwin/neo4jengine")

var (
	// rootlessAssemblyCounter count how many times do we encounter assembly without
	// a root while taking a snapshot of a digital twin. This counter will help us
	// monitor the appearances of this scenario.
	rootlessAssemblyCounter metric.Int64Counter
)

func init() {
	// We're initiating the metric instruments on the otel meter. Encounter an error
	// during an instrument's initialisation, triggering a panic. This scenario
	// should not occur, if it does, it is likely related to the attributes applied
	// on the instrument.
	var err error
	rootlessAssemblyCounter, err = meter.Int64Counter(
		"snapshot_assembly_without_root_counter",
		metric.WithDescription("how many times a digital twin snapshot has return assembly without root"),
	)
	if err != nil {
		s := fmt.Sprintf("snapshot: failed to init 'snapshot_assembly_without_root_counter' instrument: %v", err)
		panic(s)
	}
}
