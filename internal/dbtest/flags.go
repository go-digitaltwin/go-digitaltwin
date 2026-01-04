package dbtest

import (
	"flag"
	"os"
	"os/signal"
)

// Inspect can be set to prevent containers from being torn down immediately
// after the test fails. This is useful for debugging because the database can be
// manually inspected to understand the internal state after a failure.
//
// Although the test container will not be torn down, it will still be reaped by
// the testcontainers library after some time. See their documentation for more
// information.
var Inspect = flag.Bool("dbtest.inspect", false, "keep test container running for inspection after a failed test completes")

// waitForInspection blocks until the user signals that they are done inspecting
// the database by sending a SIGINT (Ctrl+C).
func waitForInspection() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	defer signal.Stop(c)
	<-c
}
