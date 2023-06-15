package tracing

type (
	MessageState interface {
		Len() int
		Has(name string) bool
		Del(name string) interface{}
		Set(name string, value interface{}) (old interface{}, err error)
		Value(name string) interface{}
		Visit(visit func(name string, value interface{}))
	}
)
