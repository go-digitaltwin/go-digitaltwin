package dbtest

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	neo4jtest "github.com/testcontainers/testcontainers-go/modules/neo4j"
)

// Neo4jImage exposes the image to use for the Neo4j container.
//
// The enterprise variant is chosen because it is the variant we use in
// production.
//
// See <https://hub.docker.com/_/neo4j> for more images.
const Neo4jImage = "docker.io/neo4j:5-enterprise"

// Default port of the transactional HTTP(S) endpoints:
// <https://neo4j.com/docs/rest-docs/current>
const (
	neo4jHTTPS = nat.Port("7473/tcp")
	neo4jHTTP  = nat.Port("7474/tcp")
)

// SetupNeo4j spins up a new Neo4j Docker container and returns a driver
// connected to it. The returned driver is closed during cleanup of the provided
// [*testing.T].
//
// The provided [*testing.T] is used to:
//   - skip the test if the '-short' flag is set,
//   - clean up the container after the test completes, and
//   - mark the test as parallel to avoid blocking other long-running tests.
//
// This is a higher-level wrapper around the functionality provided by
// testcontainers-go and its neo4j module. Use this function to avoid duplicating
// the same boilerplate code in common tests that require a standard Neo4j
// database.
//
// This function may change its definition of a "standard" Neo4j instance over
// time. If you need a specific customisation of Neo4j, you should use the
// testcontainers-go modules directly. Otherwise, you may find that your tests
// break, implying that you depend on a deployment detail no-longer considered
// "standard" and thus may break in production too.
func SetupNeo4j(t *testing.T) neo4j.DriverWithContext {
	t.Helper()

	// Container-based tests are long-running and should respect the '-short' flag.
	if testing.Short() {
		t.Skip("Skipping container-based test in short mode...")
	}

	// Always run container-based tests in parallel.
	t.Parallel()

	ctx := context.Background()

	// Spin up a database container and tear it down gracefully
	// after the test completes.
	opts := containerOptions(t,
		neo4jtest.WithoutAuthentication(),
		neo4jtest.WithAcceptCommercialLicenseAgreement(),
	)

	container, err := neo4jtest.Run(ctx, Neo4jImage, opts...)
	if err != nil {
		t.Fatal("Failed to run neo4j container:", err)
	}
	t.Cleanup(func() {
		t.Logf("Terminating neo4j container %q...", container.GetContainerID())
		if err := container.Terminate(ctx); err != nil {
			t.Error("Encountered an error during cleanup; terminate container:", err)
		}
	})

	// First, get runtime information about the container.
	boltURL, err := container.BoltUrl(ctx)
	if err != nil {
		t.Fatal("Failed to get bolt url:", err)
	}

	// Local developers may wish to connect manually to the database, so we provide a
	// URL to the browser. See
	// <https://neo4j.com/docs/browser-manual/current/operations/browser-url-parameters>
	httpEndpoint, err := container.PortEndpoint(ctx, neo4jHTTP, "http")
	if err != nil {
		t.Fatal("Failed to get http endpoint:", err)
	}

	// Connect to the container database and cleanup by the time the test ends.
	driver, err := neo4j.NewDriverWithContext(boltURL, neo4j.NoAuth())
	if err != nil {
		t.Fatal("Failed to open neo4j driver:", err)
	}
	t.Cleanup(func() {
		if err := driver.Close(ctx); err != nil {
			t.Error("Encountered an error during cleanup while closing the neo4j driver:", err)
		}
	})

	// Verify that the connection is working and the database is ready.
	if err := verifyConnectivityWithRetries(t, ctx, driver); err != nil {
		t.Fatalf("Failed to establish a connection with the remote neo4j server after retries: %v", err)
	}

	// Keep the container running for manual debugging of the graph.
	t.Cleanup(func() {
		if t.Failed() && *Inspect {
			t.Logf("Container %v is still running for inspection (Ctrl+C to terminate)...", container.GetContainerID())
			t.Logf("HTTP URL = %s/browser?preselectAuthMethod=%s&dbms=%s", httpEndpoint, url.QueryEscape("[NO_AUTH]"), url.QueryEscape(boltURL))
			t.Logf("Bolt URL = %s", boltURL)
			waitForInspection()
		}
	})

	return driver
}

// Call verifyConnectivityWithRetries to check if there is a working connection
// to Neo4j while also performing retries.
//
// In the case that the driver container returns before Neo4j is fully ready,
// it is useful to perform a limited number of connectivity retries, before
// determining that the connectivity is not working.
func verifyConnectivityWithRetries(t *testing.T, ctx context.Context, driver neo4j.DriverWithContext) error {
	t.Helper()

	const retryLimit = 5
	const retryPause = 100 * time.Millisecond

	// Initial attempt to verify the connection without a wait.
	err := driver.VerifyConnectivity(ctx)
	if err == nil {
		return nil
	}
	// Prefix each subsequent retry with a short wait.
	for r := range retryLimit {
		t.Logf("Attempting retry [%d/%d] after failing to establish a connection with the remote neo4j server: %v", r, retryLimit, err)
		// Wait, while honouring context cancellations.
		select {
		case <-time.After(retryPause):
		case <-ctx.Done():
			return fmt.Errorf("retry pause interrupted")
		}
		// Now, rerun the direct connection verification.
		err = driver.VerifyConnectivity(ctx)
		if err == nil {
			return nil
		}
	}
	return err
}
