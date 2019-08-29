package coordinator

import (
	"context"
	"github.com/feynman-go/workshop/task"
)

type Task struct {
	inner *innerTask
}

type OperationType int

const (
	OperationTypeUnknown OperationType = 0
	OperationTypeNewTask OperationType = 1
)

type Step struct {
	Type OperationType
	Desc task.Desc
}

type Scope interface {
	GetTask(ctx context.Context, taskKey string) (Task, error)
}

type Coordinator interface {
	HandleTaskUpdate(ctx context.Context, scope Scope) (Step, error)
}
