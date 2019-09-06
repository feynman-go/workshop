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

func NewFlowManager(coor Coordinator, ) *FlowManager {

}

func (manager *FlowManager) FetchFlow(flowID string, firstStage StageDesc) (*Flow, bool, error) {

}


type Repository interface {
	UpdateStage(ctx context.Context, flowID string, stage Stage) error
}

type Flow struct {
	FlowID string
	started bool
	closed bool
}

func (flow *Flow) Start(ctx context.Context) error {

}

func (flow *Flow) Wait(ctx context.Context) error {

}

func (flow *Flow) AcceptCallback(ctx context.Context, task Task, resCode int64, err error) {

}


type Coordinator interface {
	ForwardStage(ctx context.Context, stage Stage, event TaskEvent) (next *StageDesc, err error)
}

type Task struct {
	Name string
	StageType int64
	FlowSeq int64
}

type TaskEvent struct {
	Task Task
	TaskName string
}

type Operations struct {

}

func (opts Operations) ForwardStage(ctx context.Context, stage Stage, event TaskEvent) (next *StageDesc, err error) {

}

func (s *Operations) Stage(stageType int64) *StageOperation {

}

type StageOperation struct {

}

func (s *StageOperation) TaskExec(name string, executor Executor) {

}

func (s *StageOperation) TaskCallback(func(*Stage, TaskEvent) (next *StageDesc, err error)) {
	
}

type ExecPort interface {

}

type Executor func(ctx context.Context) (resCode int64, err error)