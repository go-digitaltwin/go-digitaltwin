package digitaltwin

import (
	"bytes"
	"crypto/sha1"
	"encoding"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"reflect"
	"sort"
)

// ContentAddresser is the interface describing a node (of a graph) that provides
// its own representation for hashing values based on their contents. A type that
// implements ContentAddresser has complete control over the representation of
// its data and may therefore contain things such as private fields, channels,
// and functions, which are not usually hashable in graph systems.
//
// Note: Since nodes are stored permanently, it is good design to guarantee the
// hashing used by a ContentAddresser is stable as the software evolves.
type ContentAddresser interface {
	ContentAddress(h hash.Hash) error
}

// ContentAddress returns a NodeHash for the given node.
//
// If the node implements ContentAddresser, then the hash is computed using the
// node's ContentAddress method; otherwise, the hash is computed using a
// reflection-based algorithm that hashes the node's exported fields
// (irrespective of their order).
//
// A node's content-address is tightly coupled to its stored value in the graph
// such that two nodes with the same content-address are considered equal. Put
// differently, two nodes with the same label and same property-map must have the
// same content-address.
//
// In terms of Go types, a (single) type implementing Value correlates to a
// (single) node type with its own unique label.
//
// A content-address should change if:
//
//   - the Go type changes its name
//   - the Go type moves between packages
//   - the Go type adds or removes exported fields
//   - the Go type renames an exported field
//
// A content-address should not change if:
//
//   - the Go type reorders its exported fields
//   - the Go type changes the type of its exported field, but effective values remain
//     binary-compatible (e.g. int32 to int64) - see below
//
// A content-address should be resilient to changes in exported field types,
// within the same value ranges. For example, changing a field from int32 to
// int64 should not change the content-address. However, changing a field from
// int32 to int16 should yield the same content-address only if the actual value
// hashed is within the range of the new type (i.e. the value is less than 2^15),
// otherwise the content-address must change.
func ContentAddress(node Value) (NodeHash, error) {
	h := newNodeHash(node)
	if x, ok := node.(ContentAddresser); ok {
		err := x.ContentAddress(h)
		if err != nil {
			return NodeHash{}, err
		}
	} else {
		err := reflectiveContentAddress(h, reflect.ValueOf(node))
		if err != nil {
			return NodeHash{}, err
		}
	}
	return NodeHash(h.Sum(nil)), nil
}

func reflectiveContentAddress(digest hash.Hash, node reflect.Value) error {
	if node.Kind() != reflect.Struct {
		panic("digitaltwin: reflection-based content-address supports only structs; got " + node.Kind().String())
	}

	fields := reflect.VisibleFields(node.Type())
	// sort fields by name to ensure a stable hash, regardless of the order in which
	// fields are defined in the struct.
	sort.Slice(fields, func(i, j int) bool {
		return fields[i].Name < fields[j].Name
	})

	for _, field := range fields {
		if !field.IsExported() {
			continue
		}

		// explicitly ignore embedded InformationElement fields
		if field.Name == "InformationElement" && field.Type == reflect.TypeOf(InformationElement{}) {
			continue
		}

		// hash should be different if the field name changes
		digest.Write([]byte(field.Name))

		value := node.FieldByIndex(field.Index)

		// look for a ContentAddresser implementation
		if x, ok := value.Interface().(ContentAddresser); ok {
			err := x.ContentAddress(digest)
			if err != nil {
				return fmt.Errorf("content-addresser field %s: %w", field.Name, err)
			}
			continue
		}

		// fast-path for types that implement encoding.BinaryMarshaler
		if x, ok := value.Interface().(encoding.BinaryMarshaler); ok {
			b, err := x.MarshalBinary()
			if err != nil {
				return fmt.Errorf("binary field %s: %w", field.Name, err)
			}
			digest.Write(b)
			continue
		}

		// unpack interfaces to their underlying values (if not nil interfaces)
		if value.Kind() == reflect.Interface {
			if value.IsNil() {
				// unlike pointers, nil interfaces do not have an attached type;
				// thus we cannot treat them as the zero-value of their type.
				// instead, we ignore them (writing nil to the digest is a no-op).
				continue
			}
			value = value.Elem()
		}

		// unpack pointers to their underlying values (including nil pointers)
		if value.Kind() == reflect.Ptr {
			if !value.IsNil() {
				value = value.Elem()
			} else {
				// the purpose of a content-address is to uniquely identify a node
				// based on its contents, so we must hash all fields, even if they
				// are nil pointers.
				// how do we treat a nil pointer then? after taking into account
				// field names and types (to provide uniqueness between different
				// node types) we are left to question the possible values of a
				// pointer field:
				// - nil
				// - a pointer to a zero-value of some type
				// - a pointer to a non-zero-value of some type
				// clearly the third option has inert uniqueness, but what about
				// the first two? we could treat them as the same, intentionally
				// ignoring the difference between a nil pointer and a pointer to
				// a zero-value. or we could treat them as different, but then
				// what do we hash for a nil pointer? the field name? any constant
				// value we choose is a possible value that the pointer may point to.
				// hence, we must choose to either ignore nil pointers or treat them
				// as equal to a zero-value of the pointed-to type.
				// since non-nil pointers are treated as their pointed-to values,
				// we choose to treat nil pointers as the zero-value of their type.
				value = reflect.New(value.Type().Elem()).Elem()
			}
		}

		switch value.Kind() {
		case reflect.Struct:
			// directly recurse with reflection because we know by this point
			// that the field does not implement ContentAddresser
			err := reflectiveContentAddress(digest, value)
			if err != nil {
				return fmt.Errorf("struct field %s: %w", field.Name, err)
			}
		case reflect.String:
			digest.Write([]byte(value.String()))
		case reflect.Int:
			// int is variable-size based on the architecture it is compiled for,
			// so to be consistent across architectures we convert to int64
			buf := make([]byte, binary.MaxVarintLen64)
			n := binary.PutVarint(buf, value.Int())
			digest.Write(buf[:n])
		case reflect.Uint:
			// uint is the unsigned counterpart of int, so we convert to uint64
			buf := make([]byte, binary.MaxVarintLen64)
			n := binary.PutUvarint(buf, value.Uint())
			digest.Write(buf[:n])
		case reflect.Bool, reflect.Float32, reflect.Float64,
			reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			// binary package handles fixed-size signed/unsigned integers, floats and booleans
			err := binary.Write(digest, binary.BigEndian, value.Interface())
			if err != nil {
				return fmt.Errorf("field %s: %w", field.Name, err)
			}
		case reflect.Array, reflect.Slice:
			// fast-path for numeric slices and byte-arrays
			switch value.Type().Elem().Kind() {
			case reflect.Int:
				buf := make([]byte, binary.MaxVarintLen64)
				for i := 0; i < value.Len(); i++ {
					n := binary.PutVarint(buf, value.Index(i).Int())
					digest.Write(buf[:n])
				}
			case reflect.Uint:
				buf := make([]byte, binary.MaxVarintLen64)
				for i := 0; i < value.Len(); i++ {
					n := binary.PutUvarint(buf, value.Index(i).Uint())
					digest.Write(buf[:n])
				}
			case reflect.Bool, reflect.Float32, reflect.Float64,
				reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
				reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				// all slices of numeric types are encoded as big-endian
				err := binary.Write(digest, binary.BigEndian, value.Interface())
				if err != nil {
					return fmt.Errorf("slice field %s: %w", field.Name, err)
				}
			case reflect.String:
				for i := 0; i < value.Len(); i++ {
					digest.Write([]byte(value.Index(i).String()))
				}
			default:
				// all other slice types may or may not be hashable; although we could
				// recursively call reflectiveContentAddress, we choose to not do so, as it
				// overcomplicates without any concrete use-case.
				return fmt.Errorf("field %s: unsupported slice of %v", field.Name, value.Type().Elem())
			}
		default:
			// all other value kinds are not supported
			return fmt.Errorf("field %s: unsupported %s %v", field.Name, value.Kind(), value.Type())
		}
	}

	return nil
}

func MustContentAddress(node Value) NodeHash {
	h, err := ContentAddress(node)
	if err != nil {
		panic(fmt.Sprintf("digitaltwin: un-hashable node (type %T): %v", node, err))
	}
	return h
}

// NodeHash is a consistent hash (i.e., content address) over a node's
// attributes. A NodeHash identifies a single node (of a graph) across different
// graphs (i.e., the same node can exist in multiple digital twins).
//
// A NodeHash is independent of the graph engine (e.g., neo4j) meaning this hash
// is computed over the node's content, as opposed to being assigned locally
// inside the engine.
//
// For example, do not include graph-engine metadata attributes (e.g., created
// timestamp, trace-ids) when computing this hash. Note that changing the content
// of a specific node is equivalent to removing the existing node and adding a
// new node with a new hash.
type NodeHash contentAddress

func (h NodeHash) MarshalText() ([]byte, error)     { return contentAddress(h).MarshalText() }
func (h *NodeHash) UnmarshalText(text []byte) error { return (*contentAddress)(h).UnmarshalText(text) }
func (h NodeHash) String() string                   { return "node(" + contentAddress(h).String() + ")" }
func (h NodeHash) IsZero() bool                     { return contentAddress(h).IsZero() }

// newNodeHash returns a unique hash based on the type of the given Node. Callers
// are expected to write to the returned hash.Hash in order to compute their
// identity content-address sum.
//
// The returned hash is guaranteed to completely fill a NodeHash value.
func newNodeHash(node any) hash.Hash {
	h := sha1.New()
	t := reflect.TypeOf(node) // type-preamble
	h.Write([]byte(t.PkgPath()))
	h.Write([]byte(t.Name()))
	return h
}

// ComponentID is a consistent hash (i.e., content address) over the
// root-subgraph of an Assembly; Its used specifically to reference a complete
// component uniquely.
//
// In our architecture, a component-graph may change across time yet remain the
// same "component" if those changes don't modify its roots.
//
// For example, a tree graph that looks like (:IMEI)-->(:IMSI) will retain the
// same ComponentID but change its ComponentHash as a result of connecting to
// another IMSI. It can also change its ComponentHash if the tree is expanded to
// (:IMEI)-->(:IMSI)-->(:APN).
//
// It is defined as its own type to provide a compile-time guarantee against
// misuse of ComponentHash or ForestHash.
type ComponentID contentAddress

func (h ComponentID) MarshalText() ([]byte, error) { return contentAddress(h).MarshalText() }
func (h *ComponentID) UnmarshalText(text []byte) error {
	return (*contentAddress)(h).UnmarshalText(text)
}
func (h ComponentID) String() string { return "component(" + contentAddress(h).String() + ")" }
func (h ComponentID) IsZero() bool   { return contentAddress(h).IsZero() }

// ComponentHash is a consistent hash (i.e., content address) over the entire
// Assembly. Hence, two assemblies with the same ComponentHash are equal.
//
// Different ComponentHashes are computed when the Assembly's roots change, or
// when the Assembly's nodes or edges change. To be explicit: If two assemblies
// contain the same nodes, but different edges -> their ComponentHash is
// different. If two assemblies contain the same nodes and edges, but different
// roots -> their ComponentHash is different.
type ComponentHash contentAddress

func (h ComponentHash) MarshalText() ([]byte, error) { return contentAddress(h).MarshalText() }
func (h *ComponentHash) UnmarshalText(text []byte) error {
	return (*contentAddress)(h).UnmarshalText(text)
}
func (h ComponentHash) String() string { return "assembly(" + contentAddress(h).String() + ")" }
func (h ComponentHash) IsZero() bool   { return contentAddress(h).IsZero() }

// ForestHash is a consistent hash (i.e., content address) over different graphs.
// A graph may contain none, one or more components (i.e., disjoint sub-graphs,
// also known as connectivity-component).
// A graph with the same nodes and different edges results in a different hash.
// To be consistent, sort the graph (nodes and edges) consistently.
//
// Although a component (Assembly) is a graph, it is not hashed by a ForestHash,
// rather by a ComponentHash; the same goes for a component's root-subgraph
// which is hashed by a ComponentID.
type ForestHash contentAddress

func (h ForestHash) MarshalText() ([]byte, error) { return contentAddress(h).MarshalText() }
func (h *ForestHash) UnmarshalText(text []byte) error {
	return (*contentAddress)(h).UnmarshalText(text)
}
func (h ForestHash) String() string { return "graph(" + contentAddress(h).String() + ")" }
func (h ForestHash) IsZero() bool   { return contentAddress(h).IsZero() }

// HashComponents digests the given components into a ForestHash.
// This function provides a different API than ComputeForestHash, but is
// otherwise equivalent.
func HashComponents(components map[ComponentID]ComponentHash) ForestHash {
	refs := make([]ComponentID, 0, len(components))
	for id := range components {
		refs = append(refs, id)
	}

	// lexicographic sort keeps a reproducible hash based on the content of the
	// components, without relying on the order of the input slice
	sort.Slice(refs, func(i, j int) bool {
		return bytes.Compare(refs[i][:], refs[j][:]) < 0
	})

	h := sha1.New()
	for _, ref := range refs {
		x := components[ref]
		h.Write(x[:])
	}
	return ForestHash(h.Sum(nil))
}

// contentAddress is a consistent hash primitive serving as the base for strongly
// typed hashes, like NodeHash, ForestHash, and others here-forth.
type contentAddress [sha1.Size]byte

func (h contentAddress) MarshalText() ([]byte, error) {
	text := make([]byte, hex.EncodedLen(len(h)))
	hex.Encode(text, h[:]) // always returns hex.EncodedLen(len(h)) (see hex.Encode)
	return text, nil
}

func (h *contentAddress) UnmarshalText(text []byte) error {
	n, err := hex.Decode(h[:], text)
	if err != nil {
		return fmt.Errorf("decode hex: %w", err)
	}
	if n != len(h) { // always n <= len(h[:]) (see hex.Decode)
		return fmt.Errorf("not enough bytes: %w", io.ErrUnexpectedEOF)
	}
	return nil
}

func (h contentAddress) String() string {
	return hex.EncodeToString(h[:])
}

// IsZero reports whether h is the zero value of the type.
func (h contentAddress) IsZero() bool {
	return h == contentAddress{}
}
