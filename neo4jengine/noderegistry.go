package neo4jengine

import (
	"encoding"
	"fmt"
	"reflect"
	"sync"

	"github.com/go-digitaltwin/go-digitaltwin"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// RawNode describes a digitaltwin.Value in a graph engine.
type RawNode struct {
	// The label of the node indicates its Go type, as set by Register and
	// RegisterLabel. If the neo4j.Node has several labels, only the one that
	// corresponds to the Go type is used here.
	Label string
	// A ContentAddress uniquely identifies the node within the graph; it is computed
	// from the node's attributes (i.e., its properties).
	//
	// Users of this package rarely create RawNodes manually.
	ContentAddress digitaltwin.NodeHash
	// The properties of the node bearing business value. That is, these are the
	// attributes that are relevant to the business logic.
	Props PropertyMap
	// The properties of the node that are used by the graph engine, usually for
	// manual debugging purposes.
	Metadata PropertyMap
}

type PropertyMap map[string]any

// Call newRawNode to construct a RawNode from the given neo4j.Node. This
// package's digitaltwin.GraphWriter must adhere to the conventions set in this
// function.
//
// The conventions (if you've reached this far, read the code anyway):
//
//   - The neo4j.Node must have only a single label.
//   - All values are stored as properties of the neo4j.Node.
//   - Properties starting with underscore ('_') are considered metadata, and for
//     internal use by this package only.
//   - The rest of the properties are used to populate the PropertyMap for ParseNode.
//   - RawNode.ContentAddress is stored in the metadata property and uses a string
//     returned from [digitaltwin.NodeHash.MarshalText].
func newRawNode(node neo4j.Node) (RawNode, error) {
	if len(node.Labels) != 1 {
		return RawNode{}, fmt.Errorf("node must have a single label")
	}

	raw := RawNode{
		Label:    node.Labels[0],
		Props:    make(map[string]any),
		Metadata: make(map[string]any),
	}
	for key, value := range node.Props {
		if key[0] == '_' {
			raw.Metadata[key] = value
		} else {
			raw.Props[key] = value
		}
	}
	v, ok := node.Props["_contentAddress"]
	if !ok {
		return RawNode{}, fmt.Errorf("key not found: _contentAddress")
	}
	// The _contentAddress is a string property, but we don't want to panic in case
	// this changes without us knowing (bug or otherwise).
	h, ok := v.(string)
	if !ok {
		return RawNode{}, fmt.Errorf("unexpected type: _contentAddress is %T", v)
	}

	err := raw.ContentAddress.UnmarshalText([]byte(h))
	if err != nil {
		return RawNode{}, fmt.Errorf("unmarshal content address: %w", err)
	}
	return raw, nil
}

// globalNodeRegistry is global for the entire package (hence, the entire
// process). The type system put forth by this package asserts any Go type maps
// to exactly one graph label; to support (read & write) that Go type in a graph.
var globalNodeRegistry nodeRegistry

type nodeRegistry struct {
	mLabelToType sync.Map // map[string]reflect.Type
	mTypeToLabel sync.Map // map[reflect.Type]string
}

// Register may cause panics, when used from different packages on structs
// with the same name; Prefer RegisterLabel instead.
func Register(node digitaltwin.Value) {
	rt := reflect.TypeOf(node)
	// Use localised name within package (the type's name within its package) as the
	// label. This may cause duplicates if used improperly.
	globalNodeRegistry.RegisterLabel(rt.Name(), rt)
}

// RegisterLabel is the explicit form of Register. Prefer it to overcome
// duplicate label conflicts between types with the same name within different
// packages.
func RegisterLabel(node digitaltwin.Value, label string) {
	globalNodeRegistry.RegisterLabel(label, reflect.TypeOf(node))
}

func (r *nodeRegistry) RegisterLabel(label string, rt reflect.Type) {
	// Store the label and type provided by the user
	if t, dup := r.mLabelToType.LoadOrStore(label, rt); dup && t != rt {
		panic(fmt.Sprintf("digitaltwin/engine: registering duplicate types for %q: %s != %s", label, t, rt))
	}
	// but the flattened type in the type table, since that's what decode needs.
	if l, dup := r.mTypeToLabel.LoadOrStore(rt, label); dup && l != label {
		r.mLabelToType.Delete(label) // Important to rollback.
		panic(fmt.Sprintf("digitaltwin/engine: registering duplicate names for %s: %q != %q", rt, l, label))
	}
}

// KnownLabels returns a list of all labels registered with the global node
// registry (i.e. all labels that can be used to identify a node).
func KnownLabels() []string {
	var labels []string
	globalNodeRegistry.mLabelToType.Range(func(label, _ any) bool {
		labels = append(labels, label.(string))
		return true
	})
	return labels
}

// LabelOf returns the neo4j node label pre-registered for the given type (with
// the global node registry) by a prior call to Register or RegisterLabel.
func LabelOf(rt reflect.Type) (label string, ok bool) {
	return globalNodeRegistry.LabelOf(rt)
}

func (r *nodeRegistry) TypeOf(label string) (rt reflect.Type, ok bool) {
	v, ok := r.mLabelToType.Load(label)
	if !ok {
		return nil, false
	}
	return v.(reflect.Type), true
}

func (r *nodeRegistry) LabelOf(rt reflect.Type) (label string, ok bool) {
	v, ok := r.mTypeToLabel.Load(rt)
	if !ok {
		return "", false
	}
	return v.(string), true
}

// ParseNode constructs a digitaltwin.Value from the given RawNode, decoding
// according to the labels pre-registered by Register and RegisterLabel.
func ParseNode(n RawNode) (digitaltwin.Value, error) {
	return globalNodeRegistry.ParseNode(n)
}

func (r *nodeRegistry) ParseNode(n RawNode) (digitaltwin.Value, error) {
	rt, ok := r.TypeOf(n.Label)
	if !ok {
		return nil, fmt.Errorf("unregistered label %q", n.Label) // TODO: custom error type
	}

	rv := reflect.New(rt) // Use a pointer to allow mutation by parseProperties.
	v := rv.Interface().(digitaltwin.Value)
	err := parseProperties(v, n.Props)
	if err != nil {
		return nil, fmt.Errorf("parse node props: %w", err)
	}
	// Dereference the pointer because the label registered the non-pointer type as
	// the desired type for the node.
	v = rv.Elem().Interface().(digitaltwin.Value)

	// Defensive: make sure the content address is correct (will not panic here
	// because although this is a defensive check and the error is likely to be a bug
	// in the code, the developer does not have control over the input, meaning that
	// the error may not repeat itself - for example, by manually removing a
	// problematic node from the graph).
	h, err := digitaltwin.ContentAddress(v)
	if err != nil {
		return nil, fmt.Errorf("content address: %w", err)
	}
	if h != n.ContentAddress {
		return nil, fmt.Errorf("defensive: content address mismatch: %q != %q", h.String(), n.ContentAddress)
	}

	return v, nil
}

// Call parseProperties to populate the given digitaltwin.Value according to the
// properties in the given map. It takes into account types specialised by Parser
// or uses reflection otherwise.
func parseProperties(v digitaltwin.Value, m PropertyMap) error {
	if reflect.TypeOf(v).Kind() != reflect.Pointer {
		return fmt.Errorf("called with non-pointer %T", v)
	}
	if v == nil {
		return fmt.Errorf("called with nil %T", v)
	}

	// If the value implements the Parser interface, use it.
	if parser, ok := v.(Parser); ok {
		return parser.ParseNode(m)
	}

	// Fallback to the reflection-based algorithm.
	return reflectionAdapter(reflect.ValueOf(v)).ParseNode(m)
}

// Parser is the interface implemented by types that can parse a PropertyMap of
// themselves.
//
// ParseNode must not store the map directly after returning.
//
// It is safe for Parsers to assume they are not called with a nil map. By
// convention, Parsers should implement ParseNode(empty-map) as an error.
type Parser interface {
	ParseNode(props PropertyMap) error
}

// FormatNode deconstructs the given digitaltwin.Value to a RawNode, encoding
// according to the labels pre-registered by Register and RegisterLabel.
func FormatNode(n digitaltwin.Value) (node RawNode, err error) {
	return globalNodeRegistry.FormatNode(n)
}

func (r *nodeRegistry) FormatNode(v digitaltwin.Value) (RawNode, error) {
	t := reflect.TypeOf(v)
	label, ok := r.LabelOf(t)
	if !ok {
		return RawNode{}, fmt.Errorf("unregistered type %q", t)
	}

	h, err := digitaltwin.ContentAddress(v)
	if err != nil {
		return RawNode{}, fmt.Errorf("content address: %w", err)
	}
	props, err := formatProperties(v)
	if err != nil {
		return RawNode{}, fmt.Errorf("format properties: %w", err)
	}
	return RawNode{
		Label:          label,
		ContentAddress: h,
		Props:          props,
	}, nil
}

// Formatter is the interface implemented by types to extract their properties
// to store in a graph engine.
type Formatter interface {
	FormatNode() (props PropertyMap, err error)
}

// formatProperties returns a map of node properties representing the given
// digitaltwin.Value.
func formatProperties(v digitaltwin.Value) (PropertyMap, error) {
	// If the value implements the Formatter interface, use it.
	if formatter, ok := v.(Formatter); ok {
		return formatter.FormatNode()
	}

	// The value might implement the Formatter interface on a pointer receiver.
	t := reflect.TypeOf(v)
	if reflect.PointerTo(t).Implements(formatterType) {
		rv := reflect.ValueOf(v)
		if !rv.CanAddr() {
			// WARNING! This is a hack. We need to call FormatNode on the pointer receiver,
			// but the value is not addressable. Hence, we create a new value of the same
			// type, set its value to the original value, and then call FormatNode on the
			// pointer receiver of the new value.
			rx := reflect.Indirect(reflect.New(t))
			rx.Set(reflect.ValueOf(v))
			return rx.Addr().Interface().(Formatter).FormatNode()
			// If the value is not addressable, we cannot take the address of the value to
			// call FormatNode on it. Hence, we would've fallen back to the reflection-based
			// algorithm - which is not the desired behaviour; panic instead.
			//
			// 	panic: reflect: node of type t.String() implements Formatter on a pointer receiver, but is not addressable
			//
			// This panic statement was commented out in favour of the trick mentioned above
			// it.
		}
		return rv.Addr().Interface().(Formatter).FormatNode()
	}
	// Fallback to reflection-based algorithm.
	return reflectionAdapter(reflect.ValueOf(v)).FormatNode()
}

// Used in formatProperties.
var formatterType = reflect.TypeOf((*Formatter)(nil)).Elem()

// reflectionAdapter is a wrapper around a reflected Value that implements
// Parser and Formatter.
type reflectionAdapter reflect.Value

// ParseNode implements Parser for reflection-based types.
// It sets the properties of the node from a map of field names to values.
// Unexported fields are cannot be set.
// Qualified (see FormatNode) fields may be omitted from the map, in which case
// they are left unchanged.
func (r reflectionAdapter) ParseNode(props PropertyMap) error {
	v := reflect.Value(r)
	switch v.Kind() {
	case reflect.Ptr:
		if v.IsNil() {
			return fmt.Errorf("nil pointer")
		}
		return reflectionAdapter(v.Elem()).ParseNode(props)
	case reflect.Struct:
		for field, value := range props {
			f := v.FieldByName(field)
			if !f.IsValid() {
				return fmt.Errorf("unknown field %q", field)
			}
			if !f.CanSet() {
				return fmt.Errorf("field %q is not settable", field)
			}

			// TODO: unit-test text/binary unmarshaller
			if text, ok := f.Addr().Interface().(encoding.TextUnmarshaler); ok {
				err := text.UnmarshalText([]byte(value.(string)))
				if err != nil {
					return fmt.Errorf("unmarshal text: %w", err)
				}
			} else if binary, ok := f.Addr().Interface().(encoding.BinaryUnmarshaler); ok {
				err := binary.UnmarshalBinary(value.([]byte))
				if err != nil {
					return fmt.Errorf("unmarshal binary: %w", err)
				}
			} else {
				f.Set(reflect.ValueOf(value))
			}
		}
		return nil

	// types whose base type is a primitive type (int, string, etc.)
	// does not have any fields, so we need to handle them separately.
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		fallthrough
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		fallthrough
	case reflect.Float32, reflect.Float64:
		fallthrough
	case reflect.Bool:
		fallthrough
	case reflect.String:
		value, ok := props["value"]
		if !ok {
			return fmt.Errorf("missing value field")
		}
		v.Set(reflect.ValueOf(value))
		return nil

	case reflect.Array, reflect.Slice:
		// naive implementation: assume that the array/slice contains a supported type
		values, ok := props["values"]
		if !ok {
			return fmt.Errorf("missing values field")
		}
		v.Set(reflect.ValueOf(values))
		return nil

	default:
		return fmt.Errorf("unsupported type: %v", v.Type())
	}
}

// FormatNode implements Formatter for reflection-based types. It returns the
// properties of the node as a map of field names to values. Unexported fields
// are ignored.
func (r reflectionAdapter) FormatNode() (props PropertyMap, err error) {
	v := reflect.Value(r)
	if !v.IsValid() {
		return nil, fmt.Errorf("invalid value")
	}
	props = make(PropertyMap)

	switch v.Kind() {
	case reflect.Ptr:
		return nil, fmt.Errorf("unsupported pointer type: %v", v.Type())

	case reflect.Struct:
		fields := reflect.VisibleFields(v.Type())
		for _, f := range fields {
			// skip digitaltwin.InformationElement embedded inside every digitaltwin.Value
			if f.Name == "InformationElement" && f.Type == reflect.TypeOf(digitaltwin.InformationElement{}) {
				continue
			}

			v := v.FieldByIndex(f.Index).Interface()
			// TODO: unit-test text/binary marshaller
			if text, ok := v.(encoding.TextMarshaler); ok {
				b, err := text.MarshalText()
				if err != nil {
					return nil, fmt.Errorf("marshal text: %w", err)
				}
				props[f.Name] = string(b)
			} else if binary, ok := v.(encoding.BinaryMarshaler); ok {
				b, err := binary.MarshalBinary()
				if err != nil {
					return nil, fmt.Errorf("marshal binary: %w", err)
				}
				props[f.Name] = b
			} else {
				props[f.Name] = v
			}
		}
		return props, nil

	// types whose base type is a primitive type (int, string, etc.)
	// does not have any fields, so we need to handle them separately.
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		fallthrough
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		fallthrough
	case reflect.Float32, reflect.Float64:
		fallthrough
	case reflect.Bool:
		fallthrough
	case reflect.String:
		props["value"] = v.Interface()
		return props, nil

	case reflect.Array, reflect.Slice:
		// naive implementation: assume that the array/slice contains a supported type
		props["values"] = v.Interface()
		return props, nil

	default:
		return nil, fmt.Errorf("unsupported type: %v", v.Type())
	}
}
