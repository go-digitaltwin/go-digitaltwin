package digitaltwin

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var tracer = otel.Tracer("github.com/go-digitaltwin/go-digitaltwin")
var meter = otel.Meter("github.com/go-digitaltwin/go-digitaltwin")

// ---- disassembler.go ----

const (
	// digitaltwinGraphName is the attribute key used to associate each record with
	// the corresponding digital twin graph name. This enables detailed analysis of
	// metrics, such as disassemblyDuration and disassemblyFailures, allowing both
	// collective examination across all digital twin graphs and individual analysis
	// per graph.
	digitaltwinGraphName = "digitaltwin"
)

var (
	// disassemblyDuration measures the duration of a single GraphChanged
	// disassembly, including the duration it took to produce (to pubsub service) the
	// entire set of ComponentChanged messages.
	//
	// Each record is associated with the digitaltwinGraphName.
	disassemblyDuration metric.Float64Histogram
	// disassemblyFailures measures the number of failed disassembly processes.
	//
	// Each record is associated with the digitaltwinGraphName.
	disassemblyFailures metric.Int64Counter
)

func init() {
	var err error
	disassemblyDuration, err = meter.Float64Histogram(
		"graphChanged.disassembly.duration",
		metric.WithDescription("The duration of a single GraphChanged disassembly, including the duration it took to produce (to pubsub service) the entire set of ComponentChanged messages."),
		metric.WithUnit("ms"),
	)
	if err != nil {
		panic("digitaltwin: failed to init 'graphChanged.disassembly.duration' instrument")
	}

	disassemblyFailures, err = meter.Int64Counter(
		"graphChanged.disassembly.failures",
		metric.WithDescription("The number of disassembly processes that have failed."),
	)
	if err != nil {
		panic("digitaltwin: failed to init 'graphChanged.disassembly.failures' instrument")
	}
}

// measureDisassembly measures the disassembly process using the measurements
// disassemblyDuration and disassemblyFailures. If the disassembly process
// succeeded, we record its duration. If it failed, we increment the failure
// counter.
//
// Each record, whether it's for disassembly duration or failures, is labeled
// with the relevant digital twin's graph name. This labeling allows for
// collective analysis of all disassembly processes, as well as detailed
// individual analysis for each digital twin graph.
//
// According to [metric] documentation, [metric.WithAttributeSet] should be used
// instead of [metric.WithAttributes] for performance optimization.
func measureDisassembly(ctx context.Context, graphName string, succeeded bool, d time.Duration) {
	// According to go.opentelemetry.io/otel/attribute package documentation,
	// attribute.Set should be used instead of attribute.KeyValue directly for
	// performance optimization.
	attrs := attribute.NewSet(attribute.String(digitaltwinGraphName, graphName))
	// If the disassembly process succeeded, we record its duration. If it failed, we
	// increment the failure counter.
	if succeeded {
		// We record OpenTelemetry metrics for each successful GraphChanged disassembly
		// process to gain a detailed understanding of the procedure's performance.
		//
		// We use floating-point division here for higher precision (instead of the
		// Millisecond method).
		duration := float64(d) / float64(time.Millisecond)
		disassemblyDuration.Record(ctx, duration, metric.WithAttributeSet(attrs))
	} else {
		// If the disassembly process fails, we increment the failure counter.
		disassemblyFailures.Add(ctx, 1, metric.WithAttributeSet(attrs))
	}
}
