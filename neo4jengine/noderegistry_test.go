package neo4jengine

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/go-digitaltwin/go-digitaltwin"
)

// This test ensures consistent behaviour regarding nodes representing
// digitaltwin.Value types that were not registered with the global registry.
func TestUnregisteredNode(t *testing.T) {
	// This type is never registered with the global registry.
	type Unregistered struct{ digitaltwin.InformationElement }

	var value Unregistered
	_, err := FormatNode(value)
	if err == nil {
		t.Errorf("FormatNode() = nil; want error")
	}

	node := RawNode{Label: "unregistered"}
	_, err = ParseNode(node)
	if err == nil {
		t.Errorf("ParseNode() = nil; want error")
	}
}

// Tests that the reflection adapter is not called for types that implement the
// Formatter interface using pointer receivers. See warning inside the code of
// formatProperties().
func TestFormatterWithPointerReceiver(t *testing.T) {
	var p = ptrReceiver{s: "foo"}
	props, err := formatProperties(p)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(props, PropertyMap{"s": "foo"}); diff != "" {
		t.Errorf("unexpected property map (-want +got):\n%s", diff)
	}
}

// The ptrReceiver is the type that implements the Formatter interface using
// pointer receivers. This is a common pattern in the digitaltwin package.
type ptrReceiver struct {
	digitaltwin.InformationElement // Make ptrReceiver implement digitaltwin.Value.
	// This field must remain unexpected to detect whether the reflection adapter
	// is called or not (it ignores unexpected fields).
	s string
}

func (p *ptrReceiver) FormatNode() (props PropertyMap, err error) {
	props = make(PropertyMap)
	props["s"] = p.s
	return props, nil
}

func TestReflectionAdapter(t *testing.T) {
	type testcase struct {
		name        string
		value       any
		unsupported bool // mark this value as unsupported - hence expect formatProperties to fail
	}
	var tests []testcase

	// primitive types (named builtins)
	tests = append(tests,
		testcase{
			name:  "Primitives/String",
			value: "42",
		},
		testcase{
			name:  "Primitives/Int",
			value: 42,
		},
		testcase{
			name:  "Primitives/Uint",
			value: uint(42),
		},
		testcase{
			name:  "Primitives/Float",
			value: 42.0,
		},
		testcase{
			name:  "Primitives/Bool",
			value: true,
		},
		testcase{
			name:        "Primitives/Nil",
			value:       nil,
			unsupported: true,
		},
	)

	// primitive types (unnamed builtins)
	tests = append(tests,
		testcase{
			name:  "Primitives/StringSlice",
			value: []string{"42", "43"},
		},
		testcase{
			name:  "Primitives/IntSlice",
			value: []int{42, 43},
		},
		testcase{
			name:  "Primitives/UintSlice",
			value: []uint{42, 43},
		},
		testcase{
			name:  "Primitives/FloatSlice",
			value: []float64{42.0, 43.0},
		},
		testcase{
			name:  "Primitives/BoolSlice",
			value: []bool{true, false},
		},
	)

	// underlying types
	type StringType string
	type IntType int
	type UintType uint
	type FloatType float64
	type BoolType bool
	tests = append(tests,
		testcase{
			name:  "Underlying/String",
			value: StringType("42"),
		},
		testcase{
			name:  "Underlying/Int",
			value: IntType(42),
		},
		testcase{
			name:  "Underlying/Uint",
			value: UintType(42),
		},
		testcase{
			name:  "Underlying/Float",
			value: FloatType(42.0),
		},
		testcase{
			name:  "Underlying/Bool",
			value: BoolType(true),
		},
	)

	// embedded (struct) types
	type EmbeddedString struct {
		Field           string
		UnderlyingField StringType
	}
	type EmbeddedInt struct {
		Field           int
		UnderlyingField IntType
	}
	type EmbeddedUint struct {
		Field           uint
		UnderlyingField UintType
	}
	type EmbeddedFloat struct {
		Field           float64
		UnderlyingField FloatType
	}
	type EmbeddedBool struct {
		Field           bool
		UnderlyingField BoolType
	}
	type NestedEmbedded struct {
		EmbeddedString
		EmbeddedInt
		EmbeddedUint
		EmbeddedFloat
		EmbeddedBool
	}
	tests = append(tests, testcase{
		name: "Embedded/Combined",
		value: NestedEmbedded{
			EmbeddedString: EmbeddedString{
				Field:           "42",
				UnderlyingField: "42",
			},
			EmbeddedInt: EmbeddedInt{
				Field:           42,
				UnderlyingField: 42,
			},
			EmbeddedUint: EmbeddedUint{
				Field:           42,
				UnderlyingField: 42,
			},
			EmbeddedFloat: EmbeddedFloat{
				Field:           42.0,
				UnderlyingField: 42.0,
			},
			EmbeddedBool: EmbeddedBool{
				Field:           true,
				UnderlyingField: true,
			},
		},
	})

	// anonymous (struct) types
	tests = append(tests,
		testcase{
			name:  "Anonymous/String",
			value: struct{ StringField string }{"42"},
		},
		testcase{
			name:  "Anonymous/Int",
			value: struct{ IntField int }{42},
		},
		testcase{
			name:  "Anonymous/Uint",
			value: struct{ UintField uint }{42},
		},
		testcase{
			name:  "Anonymous/Float",
			value: struct{ FloatField float64 }{42.0},
		},
		testcase{
			name:  "Anonymous/Bool",
			value: struct{ BoolField bool }{true},
		},
		testcase{
			name: "Anonymous/Composite",
			value: struct {
				StringField string
				IntField    int
				UintField   uint
				FloatField  float64
				BoolField   bool
			}{"42", 42, 42, 42.0, true},
		},
		testcase{
			name: "Anonymous/Embedded",
			value: struct {
				NestedEmbedded
			}{
				NestedEmbedded: NestedEmbedded{
					EmbeddedString: EmbeddedString{
						Field:           "42",
						UnderlyingField: "42",
					},
					EmbeddedInt: EmbeddedInt{
						Field:           42,
						UnderlyingField: 42,
					},
					EmbeddedUint: EmbeddedUint{
						Field:           42,
						UnderlyingField: 42,
					},
					EmbeddedFloat: EmbeddedFloat{
						Field:           42.0,
						UnderlyingField: 42.0,
					},
					EmbeddedBool: EmbeddedBool{
						Field:           true,
						UnderlyingField: true,
					},
				},
			},
		},
	)

	// composite types
	type CompositeType struct {
		StringField string
		IntField    int
		UintField   uint
		FloatField  float64
		BoolField   bool
		NestedField NestedEmbedded
		NestedEmbedded
		Anonymous struct{ Field NestedEmbedded }
	}
	tests = append(tests,
		testcase{
			name:  "Composite/Empty",
			value: CompositeType{},
		},
		testcase{
			name: "Composite/NonEmpty",
			value: CompositeType{
				StringField: "42",
				IntField:    42,
				UintField:   42,
				FloatField:  42.0,
				BoolField:   true,
				NestedField: NestedEmbedded{
					EmbeddedString: EmbeddedString{
						Field:           "42",
						UnderlyingField: "42",
					},
					EmbeddedInt: EmbeddedInt{
						Field:           42,
						UnderlyingField: 42,
					},
					EmbeddedUint: EmbeddedUint{
						Field:           42,
						UnderlyingField: 42,
					},
					EmbeddedFloat: EmbeddedFloat{
						Field:           42.0,
						UnderlyingField: 42.0,
					},
					EmbeddedBool: EmbeddedBool{
						Field:           true,
						UnderlyingField: true,
					},
				},
				NestedEmbedded: NestedEmbedded{
					EmbeddedString: EmbeddedString{
						Field:           "42",
						UnderlyingField: "42",
					},
					EmbeddedInt: EmbeddedInt{
						Field:           42,
						UnderlyingField: 42,
					},
					EmbeddedUint: EmbeddedUint{
						Field:           42,
						UnderlyingField: 42,
					},
					EmbeddedFloat: EmbeddedFloat{
						Field:           42.0,
						UnderlyingField: 42.0,
					},
					EmbeddedBool: EmbeddedBool{
						Field:           true,
						UnderlyingField: true,
					},
				},
				Anonymous: struct{ Field NestedEmbedded }{
					Field: NestedEmbedded{
						EmbeddedString: EmbeddedString{
							Field:           "42",
							UnderlyingField: "42",
						},
						EmbeddedInt: EmbeddedInt{
							Field:           42,
							UnderlyingField: 42,
						},
						EmbeddedUint: EmbeddedUint{
							Field:           42,
							UnderlyingField: 42,
						},
						EmbeddedFloat: EmbeddedFloat{
							Field:           42.0,
							UnderlyingField: 42.0,
						},
						EmbeddedBool: EmbeddedBool{
							Field:           true,
							UnderlyingField: true,
						},
					},
				},
			},
		},
	)

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			in := reflect.ValueOf(tt.value)
			props, err := reflectionAdapter(in).FormatNode()
			if err != nil {
				if tt.unsupported {
					if testing.Verbose() {
						t.Logf("formatNode(%#v) = %v", tt.value, err)
					}
					// if we expect this test to fail, then we're done
					return
				}
				t.Fatal("formatNode:", err)
			}

			// print the node properties for prospective developer validation
			if testing.Verbose() {
				t.Logf("Props = %#v", props)
			}

			out := reflect.Indirect(reflect.New(reflect.TypeOf(tt.value)))
			err = reflectionAdapter(out).ParseNode(props)
			if err != nil {
				t.Fatal("ParseNode:", err)
			}

			// developer validation - to make sure we're not comparing apples to oranges
			if in.Type() != out.Type() {
				panic(fmt.Sprintf("Types do not match: %v != %v", in.Type(), out.Type()))
			}

			if !cmp.Equal(tt.value, out.Interface()) {
				t.Errorf("Compare values: got %#v, want %#v", out.Interface(), tt.value)
			}
			// also compare using reflection to make sure we're not missing anything
			if in.Comparable() {
				if !in.Equal(out) {
					t.Errorf("Compare with reflection: got %#v, want %#v", out.Interface(), in.Interface())
				}
			} else {
				// some types are not comparable, so we can't use reflection to compare them
				t.Logf("Skipping reflection comparison for incomparable type: %v", in.Type())
			}
		})
	}
}
