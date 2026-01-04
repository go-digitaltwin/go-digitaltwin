package digitaltwin

// Value is the atomic unit of information of an Assembly component graph.
// Although the digitaltwin package could work with any type, we guard against
// accidental use of types by requiring them to implement this interface.
//
// Type-assert values in order to access the actual type and its fields.
//
// DO NOT forget to register your type with gob.Register() before encoding.
type Value interface {
	// digitaltwin is a no-op method that allows us to distinguish between types that
	// implement Value and those that do not.
	//
	// it is unexported to prevent implementation by types outside this package -
	// instead, these types should embed the digitaltwinDotValue struct.
	digitaltwin()
}

// InformationElement implements Value in order to embed into user-defined types
// to explicitly implement Value.
//
// Although embedding a Value field is type-equivalent to embedding this type, an
// interface field takes up 2 words of memory, whereas a field of this type takes
// up 0 words of memory.
type InformationElement struct{}

func (InformationElement) digitaltwin() {}
