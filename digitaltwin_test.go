package digitaltwin

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
)

var marshalTests = []struct {
	Name  string
	Value GraphChanged
}{
	{
		Name:  "Empty",
		Value: GraphChanged{},
	},
	{
		Name: "NoChanges",
		Value: GraphChanged{
			GraphBefore: ForestHash{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			GraphAfter:  ForestHash{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
	},
	{
		Name: "Created",
		Value: GraphChanged{
			GraphBefore: ForestHash{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			Created: []AssemblyCreated{
				{Assembly: newAssembly("1")},
				{Assembly: newAssembly("2")},
				{Assembly: newAssembly("3")},
			},
			GraphAfter: ForestHash{9, 8, 7, 6, 5, 4, 3, 2, 1, 0},
		},
	},
	{
		Name: "Updated",
		Value: GraphChanged{
			GraphBefore: ForestHash{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			Updated: []AssemblyUpdated{
				{Assembly: newAssembly("4"), Baseline: ComponentHash{0xaa}},
				{Assembly: newAssembly("5"), Baseline: ComponentHash{0xbb}},
				{Assembly: newAssembly("6"), Baseline: ComponentHash{0xcc}},
			},
			GraphAfter: ForestHash{9, 8, 7, 6, 5, 4, 3, 2, 1, 0},
		},
	},
	{
		Name: "Removed",
		Value: GraphChanged{
			GraphBefore: ForestHash{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			Removed: []AssemblyRemoved{
				{
					ID:   ComponentID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0xa, 0xb, 0xc, 0xd, 0xf},
					Hash: ComponentHash{0xaa, 0xbb, 0xcc, 0xdd, 0xff},
				},
			},
			GraphAfter: ForestHash{9, 8, 7, 6, 5, 4, 3, 2, 1, 0},
		},
	},
	{
		Name: "Everything",
		Value: GraphChanged{
			GraphBefore: ForestHash{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			Created: []AssemblyCreated{
				{Assembly: newAssembly("1")},
				{Assembly: newAssembly("2")},
				{Assembly: newAssembly("3")},
			},
			Updated: []AssemblyUpdated{
				{Assembly: newAssembly("4"), Baseline: ComponentHash{0xaa}},
				{Assembly: newAssembly("5"), Baseline: ComponentHash{0xbb}},
				{Assembly: newAssembly("6"), Baseline: ComponentHash{0xcc}},
			},
			Removed: []AssemblyRemoved{
				{
					ID:   ComponentID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0xa, 0xb, 0xc, 0xd, 0xf},
					Hash: ComponentHash{0xaa, 0xbb, 0xcc, 0xdd, 0xff},
				},
			},
			GraphAfter: ForestHash{9, 8, 7, 6, 5, 4, 3, 2, 1, 0},
		},
	},
}

func TestGobMarshalling(t *testing.T) {
	for i := range marshalTests {
		tt := marshalTests[i]
		t.Run(tt.Name, func(t *testing.T) {
			var p bytes.Buffer
			enc := gob.NewEncoder(&p)
			if err := enc.Encode(tt.Value); err != nil {
				t.Fatal("Encode(gob)", err)
			}
			var reconstructed GraphChanged
			dec := gob.NewDecoder(&p)
			if err := dec.Decode(&reconstructed); err != nil {
				t.Fatal("Decode(gob)", err)
			}

			if diff := cmp.Diff(tt.Value, reconstructed); diff != "" {
				t.Error("Reconstructed value differs:", diff)
			}
		})
	}
}

func TestJSONMarshalling(t *testing.T) {
	t.Skip("JSON marshalling is not supported (yet?)")

	for i := range marshalTests {
		tt := marshalTests[i]
		t.Run(tt.Name, func(t *testing.T) {
			p, err := json.Marshal(&tt.Value)
			if err != nil {
				t.Fatal("Marshal()", err)
			}
			var reconstructed GraphChanged
			if err := json.Unmarshal(p, &reconstructed); err != nil {
				t.Logf("Marshal() = %q", p)
				t.Fatal("Unmarshal()", err)
			}

			if diff := cmp.Diff(tt.Value, reconstructed); diff != "" {
				t.Error("Reconstructed value differs:", diff)
			}
		})
	}
}

func BenchmarkGobMarshalling(b *testing.B) {
	for i := range marshalTests {
		tt := marshalTests[i]
		b.Run(tt.Name, func(b *testing.B) {
			b.Run("Marshal", func(b *testing.B) {
				b.RunParallel(func(pb *testing.PB) {
					var p bytes.Buffer
					enc := gob.NewEncoder(&p)
					for pb.Next() {
						p.Reset()
						if err := enc.Encode(tt.Value); err != nil {
							b.Error("Encode(gob)", err)
						}
					}
				})
				b.ReportAllocs()
			})

			b.Run("Unmarshal", func(b *testing.B) {
				var p bytes.Buffer
				enc := gob.NewEncoder(&p)
				if err := enc.Encode(tt.Value); err != nil {
					b.Fatal("Encode(gob)", err)
				}
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					var reconstructed GraphChanged
					for pb.Next() {
						dec := gob.NewDecoder(bytes.NewReader(p.Bytes()))
						if err := dec.Decode(&reconstructed); err != nil {
							b.Fatal("Decode(gob)", err)
						}
					}
				})
				b.ReportAllocs()
			})
		})
	}
}

func newAssembly(unique string) Assembly {
	return AssemblyGraph{
		Root: []NodeHash{{1}},
		Vertices: map[NodeHash]Value{
			{1}: testValue{Value: unique + "1"},
			{2}: testValue{Value: unique + "2"},
			{3}: testValue{Value: unique + "3"},
			{4}: testValue{Value: unique + "3"},
		},
		Neighbours: map[NodeHash][]NodeHash{
			{2}: {{3}},
		},
	}
}

// init registers testValue for gob marshalling
func init() {
	gob.Register(testValue{})
}

type testValue struct {
	InformationElement
	Value string
}
