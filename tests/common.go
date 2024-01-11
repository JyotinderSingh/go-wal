package tests

type OperationType int

const (
	InsertOperation OperationType = iota
	DeleteOperation
)

type Record struct {
	Op    OperationType `json:"op"`
	Key   string        `json:"key"`
	Value []byte        `json:"value"`
}
