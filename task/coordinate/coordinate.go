package coordinate

import (
	"context"
	"github.com/feynman-go/workshop/task"
)

type Stage struct {
	FlowID string
	SeqId int64
	Type int64
	Tasks map[string]StageTaskStatus
}

type StageTaskStatus struct {
	Key string
	Closed bool
}

type TaskDesc struct {
	StageType int64
	Name string
	Desc task.Desc
}

type StageDesc struct {
	StageType int64
	Tasks []StageTask
}

type FlowManager struct {
	taskManager *task.Manager
}

func (manager *FlowManager) NewFlow(flowID string, firstStage StageDesc) (*Flow, error) {

}

type Flow struct {
	FlowID string
}

type Coordinator interface {
	ForwardStage(ctx context.Context, stage Stage, event TaskEvent) (next *StageDesc, err error)
}

type Task struct {
	Key string
	ExecutionID string
}

type TaskEvent struct {
	Task task.Task
	TaskName string
}

type Operations struct {

}

func (s *Operations) Stage() {

}

type StageOperation struct {

}

func (s *StageOperation) TaskExec(key string, executor task.Executor) {

}
