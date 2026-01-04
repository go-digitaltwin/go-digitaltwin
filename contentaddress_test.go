package digitaltwin

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"reflect"
	"runtime"
	"sort"
	"testing"
)

func TestContentAddress(t *testing.T) {
	// ContentAddress should return different hashes for
	// different types of values.
	type (
		SomeValue struct {
			InformationElement
			V string
		}
		OtherValue struct {
			InformationElement
			V string
		}
	)

	tests := []struct {
		Name        string
		Left, Right Value
		Equals      bool
	}{
		{
			Name:   "types=same,values=same",
			Left:   SomeValue{V: "left"},
			Right:  SomeValue{V: "left"},
			Equals: true,
		},
		{
			Name:   "types=same,values=different",
			Left:   SomeValue{V: "left"},
			Right:  SomeValue{V: "right"},
			Equals: false,
		},
		{
			Name:   "types=different,values=same",
			Left:   SomeValue{V: "left"},
			Right:  OtherValue{V: "left"},
			Equals: false,
		},
		{
			Name:   "types=different,values=different",
			Left:   SomeValue{V: "left"},
			Right:  OtherValue{V: "right"},
			Equals: false,
		},
	}

	type CompositeValue struct {
		InformationElement
		Inner Value // either SomeValue or OtherValue
	}

	tests = append(tests, struct {
		Name        string
		Left, Right Value
		Equals      bool
	}{ // TODO: left == right; because ContentAddress does not consider the type of the inner field
		Name:   "EmbeddedInterface",
		Left:   CompositeValue{Inner: SomeValue{V: "same"}},
		Right:  CompositeValue{Inner: OtherValue{V: "same"}},
		Equals: true,
	})

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			l, err := ContentAddress(tt.Left)
			if err != nil {
				t.Fatalf("ContentAddress(%#v): %v", tt.Left, err)
			}
			r, err := ContentAddress(tt.Right)
			if err != nil {
				t.Fatalf("ContentAddress(%#v): %v", tt.Left, err)
			}
			if (l == r) != tt.Equals {
				t.Errorf("ContentAddress(%#v) == ContentAddress(%#v) = %v, want %v", tt.Left, tt.Right, l == r, tt.Equals)
			}
		})
	}
}

// newNodeHash salts the hash with the runtime type of the value.
func TestContentAddress_typePreamble(t *testing.T) {
	type (
		someValue  struct{}
		otherValue struct{}
	)

	tests := []struct {
		Name        string
		Left, Right any
		Equals      bool
	}{
		{
			Name:   "SameTypes",
			Left:   someValue{},
			Right:  someValue{},
			Equals: true,
		},
		{
			Name:   "DifferentTypes",
			Left:   someValue{},
			Right:  otherValue{},
			Equals: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			l := newNodeHash(tt.Left)
			r := newNodeHash(tt.Right)
			if bytes.Equal(l.Sum(nil), r.Sum(nil)) != tt.Equals {
				t.Errorf("Compare %#v with %#v: got %v, want %v", tt.Left, tt.Right, l == r, tt.Equals)
			}
		})
	}
}

// ContentAddress should produce the same hash regardless of the order
// of the fields in a struct.
//
// for this test we call reflectiveContentAddress directly to avoid
// the type-preamble that newNodeHash adds.
func TestContentAddress_reflectionOrder(t *testing.T) {
	type (
		someOrder struct {
			A int
			B int
			C int
		}
		otherOrder struct {
			C int
			A int
			B int
		}
		anotherOrder struct {
			B int
			C int
			A int
		}
	)

	hash := func(v any) string {
		t.Helper()
		digest := sha1.New()
		// invoke reflectiveContentAddress directly to avoid the type preamble
		err := reflectiveContentAddress(digest, reflect.ValueOf(v))
		if err != nil {
			t.Fatalf("reflectiveContentAddress(%#v): %v", v, err)
		}
		return hex.EncodeToString(digest.Sum(nil))
	}

	compare := func(l, r any) {
		t.Helper()
		lh := hash(l)
		rh := hash(r)
		if lh != rh {
			t.Errorf("NodeHash(%T) != NodeHash(%T): %v != %v", l, r, lh, rh)
		}
	}

	compare(someOrder{A: 1, B: 2, C: 3}, otherOrder{A: 1, B: 2, C: 3})
	compare(someOrder{A: 1, B: 2, C: 3}, anotherOrder{A: 1, B: 2, C: 3})
	compare(otherOrder{A: 1, B: 2, C: 3}, anotherOrder{A: 1, B: 2, C: 3})
}

// ContentAddress should support all scalar builtin types and their slices.
//
// The test uses reflection to generate the types to test because the
// reflection-based implementation of ContentAddress switches on the type
// of the value, not the type of the field.
func TestContentAddress_reflectionTypes(t *testing.T) {
	types := []reflect.Type{
		// strings (with their slices and pointers)
		// reflect.TypeOf(rune(0)) // skip rune because is an alias for int32
		// reflect.TypeOf(byte(0)) // skip byte because is an alias for uint8
		// reflect.TypeOf([]byte{}) // skip []byte because is an alias for []uint8
		reflect.TypeOf(""),
		reflect.TypeOf([]string{}),
		reflect.TypeOf((*string)(nil)),
		// booleans (with their slices and pointers)
		reflect.TypeOf(false),
		reflect.TypeOf((*bool)(nil)),
		reflect.TypeOf([]bool{}),
		// signed integers (with their slices and pointers)
		reflect.TypeOf(0),
		reflect.TypeOf([]int{}),
		reflect.TypeOf((*int)(nil)),
		reflect.TypeOf(int8(0)),
		reflect.TypeOf([]int8{}),
		reflect.TypeOf((*int8)(nil)),
		reflect.TypeOf(int16(0)),
		reflect.TypeOf([]int16{}),
		reflect.TypeOf((*int16)(nil)),
		reflect.TypeOf(int32(0)),
		reflect.TypeOf([]int32{}),
		reflect.TypeOf((*int32)(nil)),
		reflect.TypeOf(int64(0)),
		reflect.TypeOf([]int64{}),
		reflect.TypeOf((*int64)(nil)),
		// unsigned integers (with their slices and pointers)
		reflect.TypeOf(uint(0)),
		reflect.TypeOf([]uint{}),
		reflect.TypeOf((*uint)(nil)),
		reflect.TypeOf(uint8(0)),
		reflect.TypeOf([]uint8{}),
		reflect.TypeOf((*uint8)(nil)),
		reflect.TypeOf(uint16(0)),
		reflect.TypeOf([]uint16{}),
		reflect.TypeOf((*uint16)(nil)),
		reflect.TypeOf(uint32(0)),
		reflect.TypeOf([]uint32{}),
		reflect.TypeOf((*uint32)(nil)),
		reflect.TypeOf(uint64(0)),
		reflect.TypeOf([]uint64{}),
		reflect.TypeOf((*uint64)(nil)),
		// floats (with their slices and pointers)
		reflect.TypeOf(float32(0)),
		reflect.TypeOf([]float32{}),
		reflect.TypeOf((*float32)(nil)),
		reflect.TypeOf(float64(0)),
		reflect.TypeOf([]float64{}),
		reflect.TypeOf((*float64)(nil)),
	}

	for _, typ := range types {
		t.Run(typ.String(), func(t *testing.T) {
			rv := reflect.New(typ).Elem()
			h, err := ContentAddress(genericNode{V: rv.Interface()})
			if err != nil {
				t.Fatalf("ContentAddress(%v): %v", typ, err)
			}
			t.Logf("Zero content-address for %v: %v", typ, h)
		})
	}
}

// genericNode is a Value with a single field of the type 'any' to help test the
// ContentAddress implementation.
type genericNode struct {
	V any
}

func (genericNode) digitaltwin() {}

func BenchmarkSHA1(b *testing.B) {
	type SHA1 [sha1.Size]byte

	b.Run("Compare", func(b *testing.B) {
		l := SHA1{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
		r := SHA1{255, 254, 253, 252, 251, 250, 249, 248, 247, 246, 245, 244, 243, 242, 241, 240, 239, 238, 237, 236}
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			var doNotOptimise int // see https://github.com/golang/go/issues/27400
			for pb.Next() {
				doNotOptimise = bytes.Compare(l[:], r[:])
			}
			runtime.KeepAlive(doNotOptimise)
		})
	})

	allocs := []int{0, 1, 2, 16, 512, 4096}
	for _, count := range allocs {
		// items are populated before the sub-benchmark so that each
		items := make([]SHA1, count)
		for i := 0; i < len(items); i++ {
			h := sha1.New()
			err := binary.Write(h, binary.BigEndian, int64(i)) // can only use binary.Write with fixed-size types
			if err != nil {
				b.Logf("Binary encoding value %q as big-endian", i)
				b.Fatal("Failed to write sha1:", err)
			}
			items[i] = [sha1.Size]byte(h.Sum(nil))
		}
		b.Run(fmt.Sprintf("sort.Slice(len=%d)", count), func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				scratch := make([]SHA1, len(items)) // disposable
				b.ResetTimer()
				for pb.Next() {
					// sort 'scratch' instead of 'items' because this keeps the
					// raw input intact for the next benchmark iteration
					sort.Slice(scratch, func(i, j int) bool {
						return bytes.Compare(items[i][:], items[j][:]) < 0
					})
				}
			})
			b.ReportAllocs()
		})
	}
}

func TestHashComponents(t *testing.T) {
	tests := []struct {
		name       string
		assemblies map[ComponentID]ComponentHash
		want       ForestHash
	}{
		{
			name: "empty",
			want: mustParseHash("da39a3ee5e6b4b0d3255bfef95601890afd80709"),
		},
		{
			name: "single",
			assemblies: map[ComponentID]ComponentHash{
				{1, 2, 3}: {4, 5, 6},
			},
			want: mustParseHash("3d6b7a1ab3486067137f246f9d173291a108e87f"),
		},
		{
			name: "multiple",
			assemblies: map[ComponentID]ComponentHash{
				{1, 2, 3}: {4, 5, 6},
				{7, 8, 9}: {10, 11, 12},
			},
			want: mustParseHash("40b4ed6dcc81f074edb81dc3aa00712c4f5930a3"),
		},
		{
			name: "multiple-reversed",
			assemblies: map[ComponentID]ComponentHash{
				{7, 8, 9}: {10, 11, 12},
				{1, 2, 3}: {4, 5, 6},
			},
			want: mustParseHash("40b4ed6dcc81f074edb81dc3aa00712c4f5930a3"), // same as "multiple"
		},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			got := HashComponents(tt.assemblies)
			if got != tt.want {
				t.Errorf("HashComponents() = %v, want %v", got, tt.want)
			}
		})
	}
}

func mustParseHash(s string) ForestHash {
	h, err := hex.DecodeString(s)
	if err != nil {
		panic("mustParseHash: decode hex string: " + err.Error())
	}
	return ForestHash(h)
}

func BenchmarkHashComponents(b *testing.B) {
	suites := []struct {
		name string
		size int
	}{
		{"meaningless", 2},
		{"tiny", 128},           // increase load by factor of 64
		{"small", 4096},         // increase load by factor of 32
		{"medium", 65536},       // increase load by factor of 16
		{"large", 524_288},      // increase load by factor of 8
		{"huge", 2_097_152},     // increase load by factor of 4
		{"gigantic", 4_194_304}, // increase load by factor of 2
	}
	for _, bb := range suites {
		b.Run(bb.name, func(b *testing.B) {
			assemblies := make(map[ComponentID]ComponentHash, bb.size)
			for i := 0; i < bb.size; i++ {
				var id ComponentID
				var hash ComponentHash
				binary.PutVarint(id[:], int64(i))
				binary.PutVarint(hash[:], int64(i))

				assemblies[id] = hash
			}
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					HashComponents(assemblies)
				}
			})
		})
	}
}
