package watch

type Operation int

const (
	ADD Operation = iota
	REMOVE
)

type ServiceUpdate struct {
	Key   string
	Value string
	Op    Operation
}
