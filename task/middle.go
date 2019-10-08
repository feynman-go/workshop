package task

import (
	"context"
	"fmt"
	"github.com/feynman-go/workshop/record"
)

type Middle interface {
	WrapScheduler(scheduler Scheduler) Scheduler
	WrapExecutor(executor Executor) Executor
}

func NewRecorderMiddle(factory record.Factory) Middle {
	return &recorderMiddle{
		factory: factory,
	}
}

type recorderMiddle struct {
	factory record.Factory
}

func (mid *recorderMiddle) WrapScheduler(scheduler Scheduler) Scheduler {
	return recorderWrapper{
		factory: mid.factory,
		scheduler: scheduler,
	}
}

func (mid *recorderMiddle) WrapExecutor(executor Executor) Executor {
	return recorderWrapper{
		factory: mid.factory,
		executor: executor,
	}
}

type recorderWrapper struct {
	factory record.Factory
	executor Executor
	scheduler Scheduler
}

func (wrapper recorderWrapper) ScheduleTask(ctx context.Context, task Task, cover bool) (err error) {
	recorder, ctx := wrapper.factory.ActionRecorder(ctx, "ScheduleTask")
	defer func() {
		recorder.Commit(err)
	}()
	err = wrapper.scheduler.ScheduleTask(ctx, task, cover)
	return
}

func (wrapper recorderWrapper) RemoveTaskSchedule(ctx context.Context, task Task) (err error) {
	recorder, ctx := wrapper.factory.ActionRecorder(ctx, "RemoveTaskSchedule")
	defer func() {
		recorder.Commit(err)
	}()
	err = wrapper.scheduler.RemoveTaskSchedule(ctx, task)
	return
}

func (wrapper recorderWrapper) WaitTaskAwaken(ctx context.Context) (awaken Awaken, err error) {
	recorder, ctx := wrapper.factory.ActionRecorder(ctx, "WaitTaskAwaken")
	defer func() {
		recorder.Commit(err)
	}()
	awaken, err = wrapper.scheduler.WaitTaskAwaken(ctx)
	return
}

func (wrapper recorderWrapper) ReadTask(ctx context.Context, taskKey string) (*Task, error) {
	return wrapper.scheduler.ReadTask(ctx, taskKey)
}

func (wrapper recorderWrapper) NewStageID(ctx context.Context, taskKey string) (stageID int64, err error) {
	return wrapper.scheduler.NewStageID(ctx, taskKey)
}

func(wrapper recorderWrapper) Close(ctx context.Context) error {
	return wrapper.scheduler.Close(ctx)
}

func(wrapper recorderWrapper) Execute(ctx Context, res *Result) {
	recorder, runCtx := wrapper.factory.ActionRecorder(ctx.Context, "execute")
	ctx.Context = runCtx
	defer func() {
		r := recover()
		if r != nil {
			recorder.Commit(fmt.Errorf("recover: %v", r))
		} else {
			recorder.Commit(nil)
		}
	}()
	wrapper.executor.Execute(ctx, res)
}