package digitaltwin_test

import (
	"fmt"
	"net/netip"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"gocloud.dev/pubsub"

	"github.com/danielorbach/go-component"
	. "github.com/go-digitaltwin/go-digitaltwin"
)

func TestAttributeMap(t *testing.T) {
	// Defining a single Assembly to be used across all subtests. The Assembly
	// consists of a 'node' type that represents elements within the Assembly
	// structure, featuring a single root with two child nodes.
	type node struct {
		InformationElement
		N int
	}
	var builder AssemblyBuilder
	builder.Roots(node{N: 1})
	builder.Connect(node{N: 1}, node{N: 2})
	builder.Connect(node{N: 1}, node{N: 3})
	assembly := builder.Assemble()

	t.Run("primitive", func(t *testing.T) {
		want := 1

		m := NewAttributeMap(func(assembly Assembly) (int, bool) {
			root := assembly.Roots()[0]
			n := assembly.Value(root).(node)
			return n.N, true
		}, nil)

		_, ok := m.Find(assembly.AssemblyID())
		if ok {
			t.Errorf("Find(empty map) = true, expected false")
		}

		m.Update(assembly)

		got, ok := m.Find(assembly.AssemblyID())
		if !ok {
			t.Errorf("Find(%s) not found", assembly.AssemblyID())
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("Find(%s) mismatch (-want +got):\n%s", assembly.AssemblyID(), diff)
		}
	})

	t.Run("pointer to primitive", func(t *testing.T) {
		want := 1

		m := NewAttributeMap(func(assembly Assembly) (*int, bool) {
			roots := assembly.Roots()
			for _, root := range roots {
				n, ok := assembly.Value(root).(node)
				if ok {
					return &n.N, true
				}
			}
			return nil, false
		}, nil)

		_, ok := m.Find(assembly.AssemblyID())
		if ok {
			t.Errorf("Find(empty map) = true, expected false")
		}

		m.Update(assembly)

		got, ok := m.Find(assembly.AssemblyID())
		if !ok {
			t.Errorf("Find(%s) not found", assembly.AssemblyID())
		}
		if diff := cmp.Diff(want, *got); diff != "" {
			t.Errorf("Find(%s) mismatch (-want +got):\n%s", assembly.AssemblyID(), diff)
		}
	})

	t.Run("slice", func(t *testing.T) {
		want := []int{1, 2, 3}

		m := NewAttributeMap(func(assembly Assembly) ([]int, bool) {
			var vs []int
			Inspect(assembly, func(value Value) bool {
				n, ok := value.(node)
				if ok {
					vs = append(vs, n.N)
				}
				return true
			})
			return vs, len(vs) > 0
		}, nil)

		_, ok := m.Find(assembly.AssemblyID())
		if ok {
			t.Errorf("Find(empty map) = true, expected false")
		}

		m.Update(assembly)

		got, ok := m.Find(assembly.AssemblyID())
		if !ok {
			t.Errorf("Find(%s) not found", assembly.AssemblyID())
		}
		if diff := cmp.Diff(want, got, cmpopts.SortSlices(func(a, b int) bool { return a < b })); diff != "" {
			t.Errorf("Find(%s) mismatch (-want +got):\n%s", assembly.AssemblyID(), diff)
		}
	})

	t.Run("map", func(t *testing.T) {
		want := map[int]int{2: 1, 3: 1}

		m := NewAttributeMap(func(assembly Assembly) (map[int]int, bool) {
			m := make(map[int]int)
			rootHash := assembly.Roots()[0]
			rootValue := assembly.Value(rootHash).(node).N
			for _, k := range assembly.EdgesOf(rootHash) {
				childValue := assembly.Value(k).(node).N
				m[childValue] = rootValue
			}
			return m, len(m) > 0
		}, nil)

		_, ok := m.Find(assembly.AssemblyID())
		if ok {
			t.Errorf("Find(empty map) = true, expected false")
		}

		m.Update(assembly)

		got, ok := m.Find(assembly.AssemblyID())
		if !ok {
			t.Errorf("Find(%s) not found", assembly.AssemblyID())
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("Find(%s) mismatch (-want +got):\n%s", assembly.AssemblyID(), diff)
		}
	})

	t.Run("struct", func(t *testing.T) {
		type data struct {
			Root  int
			Child []int
		}

		want := data{Root: 1, Child: []int{2, 3}}

		m := NewAttributeMap(func(assembly Assembly) (data, bool) {
			var d data
			rootNodeHase := assembly.Roots()[0]
			d.Root = assembly.Value(rootNodeHase).(node).N
			for _, k := range assembly.EdgesOf(rootNodeHase) {
				childValue := assembly.Value(k).(node).N
				d.Child = append(d.Child, childValue)
			}
			return d, d.Root > 0
		}, nil)

		_, ok := m.Find(assembly.AssemblyID())
		if ok {
			t.Errorf("Find(empty map) = true, expected false")
		}

		m.Update(assembly)

		got, ok := m.Find(assembly.AssemblyID())
		if !ok {
			t.Errorf("Find(%s) not found", assembly.AssemblyID())
		}
		if diff := cmp.Diff(want, got, cmpopts.SortSlices(func(a, b int) bool { return a < b })); diff != "" {
			t.Errorf("Find(%s) mismatch (-want +got):\n%s", assembly.AssemblyID(), diff)
		}
	})
}

// This example illustrates how to use NewAttributeMap in conjunction with
// the Iter method to transfer data between maps. It shows the process of
// initializing an AttributeMap, utilizing Iter to copy data, and then
// creating a new AttributeMap with the copied data.
func ExampleNewAttributeMap() {
	// Initial map 'm1' is created, which will be the source data for our first
	// AttributeMap.
	m1 := map[ComponentID]string{
		{0x01}: "1",
		{0x02}: "2",
		{0x03}: "3",
	}

	// In this example, a no-op AttributeFunc is created. The actual functionality of
	// the AttributeFunc is not the focus of this example
	fn := func(assembly Assembly) (string, bool) {
		return "", false
	}

	// Creating the first AttributeMap using m1 to load the initial key-value pairs.
	am1 := NewAttributeMap(fn, m1)

	// Iter is utilized to transfer all entries from am1 to a new map 'm2'
	m2 := make(map[ComponentID]string)
	am1.Iter(func(k ComponentID, v string) bool {
		m2[k] = v
		return true
	})

	// Now, we will create the second AttributeMap with the new stored values within
	// m2. Remember that the AttributeFunc must match the loaded map, so we are using
	// the same AttributeFunc as we defined in the first AttributeMap
	am2 := NewAttributeMap(fn, m2)

	// We can validate that the store and load procedure was successful by iterating over the second AttributeMap,
	// and make sure that all the keys are found and have the same value as in the original map 'm1'
	am2.Iter(func(k ComponentID, v1 string) bool {
		v2, ok := m1[k]
		fmt.Printf("Key found=%t, value equal=%t\n", ok, v1 == v2)
		return true
	})

	// Output:
	// Key found=true, value equal=true
	// Key found=true, value equal=true
	// Key found=true, value equal=true
}

// This example demonstrates the usage of the AttributeMap. As AttributeMap
// correlates between assemblies and corresponding attribute value, For this
// example, we will create a simplistic assembly structure with a root node of
// type 'device' and child node of type 'net' which contain an IP address.
func ExampleAttributeMap() {
	type net struct {
		InformationElement
		IP netip.Addr
	}
	type device struct {
		InformationElement
		Name string
	}

	createAssembly := func(name string, addr netip.Addr) Assembly {
		var builder AssemblyBuilder
		builder.Roots(device{Name: name})
		builder.Connect(device{Name: name}, net{IP: addr})
		return builder.Assemble()
	}

	// For this example, distinct assemblies with different device names and IP
	// addresses.
	assemblies := []Assembly{
		createAssembly("A", netip.MustParseAddr("1.1.1.1")),
		createAssembly("B", netip.MustParseAddr("1.1.1.2")),
		createAssembly("C", netip.MustParseAddr("3.3.3.3")),
	}

	// In this example, we are interested in the IP address of the device.
	// Specifically, devices where have IP address with in our subnet range. So we
	// will crate an AttributeFunc that determent if an assembly is valid by checking
	// if his IP address is with in our wanted subnet.
	subnet := netip.MustParsePrefix("1.1.1.0/24")
	fn := func(assembly Assembly) (netip.Addr, bool) {
		// Usually, users will visit the assembly to extract the relevant information
		// stored within.we are interested in the IP address.
		var ip netip.Addr
		Inspect(assembly, func(value Value) bool {
			n, ok := value.(net)
			if ok {
				ip = n.IP
				return false
			}
			return true
		})
		// According to the above-mentioned definition, only IP addresses within the
		// specific subnet are considered valid values of the attribute.
		return ip, subnet.Contains(ip)
	}
	// Here we can see the initialisation of AttributeMap; it is common practice to
	// define the AttributeFunc as an anonymous function within the initialisation of
	// AttributeMap, although in this example we defined the function above and pass
	// it here.
	//
	// Remember that the AttributeFunc defines what attribute will be stored in
	// AttributeMap.
	m := NewAttributeMap(fn, nil)

	// Initially, the AttributeMap is empty, as no assemblies have been stored yet.
	// We expect not to find any attribute values for the pre-constructed assemblies.
	fmt.Printf("Checking empty AttributeMap\n")
	for _, assembly := range assemblies {
		assemblyID := assembly.AssemblyID()
		attribute, ok := m.Find(assemblyID)
		fmt.Printf("assembly=%s, ok=%t, attribute=%s\n", assemblyID, ok, attribute)
	}

	// Now we will update the map with our per-construct assemblies, updating the
	// AttributeMap is safe for concurrent use, so we can update all of our
	// per-construct assemblies from multiple goroutine
	var wg sync.WaitGroup
	for i := range assemblies {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			a := assemblies[i]
			m.Update(a)
		}(i)
	}
	wg.Wait()

	// After updating, query the attribute values again.
	//
	// Note that the last assembly we defiend here:
	//	`createAssembly("C", netip.MustParseAddr("3.3.3.3"))`
	//
	// Is not meeting the AttributeFunc (this device ip is not in our wanted subnet
	// '1.1.1.0/24'.) we expect it to be absent from the AttributeMap.
	//
	// Remember that also Find is safe for concurrent use, but to make sure this
	// example output will be consistent, we will use Find sequentially.
	fmt.Printf("\nChecking AttributeMap after updating assemblies\n")
	for _, assembly := range assemblies {
		assemblyID := assembly.AssemblyID()
		attribute, ok := m.Find(assemblyID)
		fmt.Printf("assembly=%s, ok=%t, attribute=%s\n", assemblyID, ok, attribute)
	}

	// If an assembly's attribute changes, we can update it in the map to reflect the
	// change, since the AttributeMap maintains correspondence between an assembly and
	// its last known attribute value.
	x := createAssembly("A", netip.MustParseAddr("1.1.1.4"))
	m.Update(x)
	attribute, ok := m.Find(x.AssemblyID())
	fmt.Printf("\nChecking assembly after changed attribute\n")
	fmt.Printf("assembly=%s, ok=%t, attribute=%s\n", x.AssemblyID(), ok, attribute)

	// If the Assembly's attribute value is deemed invalid by the AttributeFunc, The
	// assembly will remove from the map
	y := createAssembly("A", netip.MustParseAddr("4.4.4.4"))
	m.Update(y)
	attribute, ok = m.Find(y.AssemblyID())
	fmt.Printf("\nChecking removed assembly\n")
	fmt.Printf("assembly=%s, ok=%t, attribute=%s\n", y.AssemblyID(), ok, attribute)

	// Output:
	// Checking empty AttributeMap
	// assembly=component(008772775d932cb34a0924fe37cab03d879abf8c), ok=false, attribute=invalid IP
	// assembly=component(064b63107fc4a36a2e97bb90f4d88dcfe7d5137d), ok=false, attribute=invalid IP
	// assembly=component(b3dc7cb717e5de7a2461cef1f9c96c178ebcc693), ok=false, attribute=invalid IP
	//
	// Checking AttributeMap after updating assemblies
	// assembly=component(008772775d932cb34a0924fe37cab03d879abf8c), ok=true, attribute=1.1.1.1
	// assembly=component(064b63107fc4a36a2e97bb90f4d88dcfe7d5137d), ok=true, attribute=1.1.1.2
	// assembly=component(b3dc7cb717e5de7a2461cef1f9c96c178ebcc693), ok=false, attribute=invalid IP
	//
	// Checking assembly after changed attribute
	// assembly=component(008772775d932cb34a0924fe37cab03d879abf8c), ok=true, attribute=1.1.1.4
	//
	// Checking removed assembly
	// assembly=component(008772775d932cb34a0924fe37cab03d879abf8c), ok=false, attribute=invalid IP
}

// The following example demonstrates the flow of using TrackAttribute function
// to monitor attribute changes on assemblies within a digital twin graph. This
// code is for illustration purposes only and is not meant to be executed as is.
func ExampleTrackAttribute() {
	// Normally, a component is given a linker that is used to open an interest
	// to the appropriate target of a digital-twin component. For this example,
	// we assume the outcome of that process is stored at the following variable.
	var graphChanges *pubsub.Subscription

	// Initialize a new AttributeMap to track a string attribute by using the given
	// AttributeFunc that extracts a string attribute from an assembly if
	// available
	m := NewAttributeMap[any](func(assembly Assembly) (any, bool) {
		// here should be the implementation of how to visit an Assembly and extract the
		// wanted attrbuite.
		return "", false
	}, nil)

	// Start the component process to observe attributes using TrackAttribute.
	component.RunProc(func(l *component.L) {
		l.Fork("track attribute", TrackAttribute[any](&m, graphChanges))
		l.Go("somthing to do", func(l *component.L) {
			// Retrieve and display the attribute for the given AssemblyID.
			v, ok := m.Find(ComponentID{})
			if ok {
				l.Logf("AssemblyID %s has attribute %s", ComponentID{}.String(), v)
			}
		})
	})
}
