package dbtest

import (
	"testing"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/log"
	"github.com/testcontainers/testcontainers-go/wait"
)

// A utility function to create a slice of options for a container with the given
// image and a logger that logs to the given [testing.TB].
func containerOptions(tb testing.TB, opts ...testcontainers.ContainerCustomizer) []testcontainers.ContainerCustomizer {
	customizers := make([]testcontainers.ContainerCustomizer, 0, len(opts)+1)
	customizers = append(customizers, testcontainers.WithLogger(log.TestLogger(tb)))
	return append(customizers, opts...)
}

// WithWaitForExposedPort sets the wait strategy for a container to wait for its
// exposed port to be available.
//
// Use this function with containers that do not already wait for their exposed
// port. Sometimes, tests using such containers fail spontaneously due to the
// test running before the container is actually ready.
//
// Usually, database containers only expose a single port. Do not use this
// function with database containers exposing more than a single port.
//
// We plan to open a pull-request for with testcontainers/testcontainers-go.
// Until then, this function will allow us to test the proposed solution.
func WithWaitForExposedPort() testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		strategies := []wait.Strategy{wait.ForExposedPort()}
		// Extend already set wait strategy.
		if req.WaitingFor != nil {
			strategies = append(strategies, req.WaitingFor)
		}
		// Rely on the official WaitStrategy customizer for setting the wait strategies
		// appropriately.
		err := testcontainers.WithWaitStrategy(strategies...).Customize(req)
		if err != nil {
			return err
		}
		return nil
	}
}
