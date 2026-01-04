package enginetest

import (
	"fmt"

	"github.com/google/go-cmp/cmp"

	"github.com/go-digitaltwin/go-digitaltwin"
)

// A check is any function that returns unexpected problems with the given
// [digitaltwin.GraphChanged].
type check func(digitaltwin.GraphChanged) (problem string)

// Checks that the created graph-components are exactly as expected.
//
// We identify graph components by their digitaltwin.ComponentID, and compare
// their contents using their digitaltwin.ComponentHash.
func created(components ...digitaltwin.AssemblyRef) check {
	return func(changed digitaltwin.GraphChanged) string {
		if len(changed.Created) != len(components) {
			return fmt.Sprintf("len(.Created) = %v, want %v", len(changed.Created), len(components))
		}

		// Slices are not friendly to compare by maps are (using cmp.Diff).
		var (
			want = make(map[digitaltwin.ComponentID]digitaltwin.ComponentHash)
			got  = make(map[digitaltwin.ComponentID]digitaltwin.ComponentHash)
		)
		for _, a := range components {
			want[a.AssemblyID()] = a.AssemblyHash()
		}
		for _, a := range changed.Created {
			got[a.AssemblyID()] = a.AssemblyHash()
		}

		if diff := cmp.Diff(want, got); diff != "" {
			return fmt.Sprintf("Created mismatch (-want +got):\n%v", diff)
		}
		return ""
	}
}

// Checks that the updated graph-components are exactly as expected.
//
// We identify graph components by their digitaltwin.ComponentID, and compare
// their contents using their digitaltwin.ComponentHash.
func updated(components ...digitaltwin.AssemblyRef) check {
	return func(changed digitaltwin.GraphChanged) string {
		if len(changed.Updated) != len(components) {
			return fmt.Sprintf("len(.Updated) = %v, want %v", len(changed.Updated), len(components))
		}

		// Slices are not friendly to compare by maps are (using cmp.Diff).
		var (
			want = make(map[digitaltwin.ComponentID]digitaltwin.ComponentHash)
			got  = make(map[digitaltwin.ComponentID]digitaltwin.ComponentHash)
		)
		for _, a := range components {
			want[a.AssemblyID()] = a.AssemblyHash()
		}
		for _, a := range changed.Updated {
			got[a.AssemblyID()] = a.AssemblyHash()
		}

		if diff := cmp.Diff(want, got); diff != "" {
			return fmt.Sprintf("Updated mismatch (-want +got):\n%v", diff)
		}
		return ""
	}
}

// Checks that the removed graph-components are exactly as expected.
//
// We identify graph components by their digitaltwin.ComponentID, and compare
// their contents using their digitaltwin.ComponentHash.
func removed(components ...digitaltwin.AssemblyRef) check {
	return func(changed digitaltwin.GraphChanged) string {
		if len(changed.Removed) != len(components) {
			return fmt.Sprintf("len(.Removed) = %v, want %v", len(changed.Removed), len(components))
		}

		// Slices are not friendly to compare by maps are (using cmp.Diff).
		var (
			want = make(map[digitaltwin.ComponentID]digitaltwin.ComponentHash)
			got  = make(map[digitaltwin.ComponentID]digitaltwin.ComponentHash)
		)
		for _, a := range components {
			want[a.AssemblyID()] = a.AssemblyHash()
		}
		for _, a := range changed.Removed {
			got[a.AssemblyID()] = a.AssemblyHash()
		}

		if diff := cmp.Diff(want, got); diff != "" {
			return fmt.Sprintf("Removed mismatch (-want +got):\n%v", diff)
		}
		return ""
	}
}

// A snapshot is used by sequential test-cases to check a sequence of discrete
// graph snapshots.
//
// A single snapshot contains a list of assembly refs (component-id and
// component-hash) that are expected after a single test-case (set in
// testCase.graph).
type snapshot []digitaltwin.AssemblyRef

// Checks returns the checks to perform on every two consecutive snapshots.
//
// The returned checks already include an explanation of the expected behaviour,
// regardless of the individual test-case's explanation.
func (after snapshot) Checks(before snapshot) []check {
	// We check that this snapshot directly follows the previous snapshot.
	continuousDiff := func(changed digitaltwin.GraphChanged) string {
		if h := digitaltwin.ComputeForestHash(before...); changed.GraphBefore != h {
			return fmt.Sprintf(".GraphBefore = %v, want %v: discontinuity", changed.GraphBefore, h)
		}
		return ""
	}

	// We check the graph identified by this snapshot was indeed the graph on which
	// the given changes were computed.
	expectedGraph := func(changed digitaltwin.GraphChanged) string {
		if h := digitaltwin.ComputeForestHash(after...); changed.GraphAfter != h {
			return fmt.Sprintf(".GraphAfter = %v, want %v: unexpected graph", changed.GraphAfter, h)
		}
		return ""
	}

	// Also, we check that the WhatChangeder has set the timestamp to any non-zero
	// value. We do not care about the exact timestamps, just that those are present.
	hasTimestamp := func(changed digitaltwin.GraphChanged) string {
		if changed.Timestamp.IsZero() {
			return ".Timestamp is zero: a WhatChangeder should timestamp the changes"
		}
		return ""
	}

	return []check{continuousDiff, expectedGraph, hasTimestamp}
}
