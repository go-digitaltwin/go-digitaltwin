# Golang Digital Twin

[![Go Reference](https://pkg.go.dev/badge/github.com/go-digitaltwin/go-digitaltwin.svg)](https://pkg.go.dev/github.com/go-digitaltwin/go-digitaltwin)
[![Go Report Card](https://goreportcard.com/badge/github.com/go-digitaltwin/go-digitaltwin)](https://goreportcard.com/report/github.com/go-digitaltwin/go-digitaltwin)
[![GitHub License](https://img.shields.io/github/license/go-digitaltwin/go-digitaltwin)](LICENSE)

This library offers a comprehensive module for constructing digital twins and engines to deploy them effectively.

## What is a Digital Twin?

A _digital twin_ is a virtual representation of a real-world entity. It is created by integrating event streams from various sources to generate a coherent and consistent view of the system of interest.

A digital twin maintains a simple graph structure (a directed acyclic graph without edge weights or attributes). Disjoint sub-graphs (also known as components) within this structure represent entities in the system of interest. Nodes within these components represent properties of the entities, while the edges illustrate the containment relationships between these properties.

Each component and node is identified by a unique identifier (i.e., ComponentID) and a hash of its graph (i.e., ComponentHash) versions its revisions.

## Engines

This library includes various engines that leverage several backend elements as their databases to maintain a digital twin. Currently, we provide the following engine:

- **neo4jengine**: Uses Neo4j as the database backend.

## Acknowledgments

Special thanks to [@ofektavor], [@yuvalmendelovich], [@marombracha], [@liadmor], and [@GalShnapp] for your contributions and collaboration on this project.

A very special thank you to [@sgebbie]; many of the concepts in this project came from working together and drawing from your deep experience in systems engineering.

A very special thank you to [@tal-shani] for being there from the beginning. Your unwavering support, thoughtful guidance, and genuine partnership have been invaluable throughout this journey.

Working with you all was a pleasure, and your fingerprints are all over the good parts of this codebase. Cheers!

[@ofektavor]: https://github.com/ofektavor
[@yuvalmendelovich]: https://github.com/yuvalmendelovich
[@marombracha]: https://github.com/marombracha
[@liadmor]: https://github.com/liadmor
[@GalShnapp]: https://github.com/GalShnapp
[@sgebbie]: https://github.com/sgebbie
[@tal-shani]: https://github.com/tal-shani
