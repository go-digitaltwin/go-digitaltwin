// Package digitaltwin provides a library for building digital twins; A digital
// twin is a virtual representation of a real-world entity - maintained by
// digesting event-streams from various sources in order to produce a consistent
// view about the system-of-interest.
//
// Specifically, a digital twin maintains a trivial graph (directed acyclic graph
// without edge weights or attributes) such that disjoint sub-graphs (a.k.a.
// components) represent entities in the system-of-interest and nodes within
// those graph-components represent properties of those entities; edges represent
// containment relationships between these properties.
//
// Each component is identified by a unique identifier (i.e., ComponentID) and
// a hash of its graph (i.e., ComponentHash) versions its revisions.
//
// This package exposes ... (TODO)
package digitaltwin
