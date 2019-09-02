package task

import (
	"context"
	"github.com/feynman-go/workshop/promise"
	"hash"
	"hash/crc32"
	"log"
	"math/rand"
	"sync"
	"time"
)

type Executor interface {
	StartExecution(cb Context) error
}

type FuncExecutor func(cb Context) error

func (fe FuncExecutor) StartExecution(cb Context) error {
	return fe(cb)
}

/*type ExecutionRepository interface {
	NewExecutionID(ctx context.Context, taskKey string) (int64, error)
	FindExecution(ctx context.Context, taskKey string, executionID int64) (*Execution, error)
	UpdateExecution(ctx context.Context, execution *Execution) error
}*/

type Scheduler interface {
	ScheduleTask(ctx context.Context, task Task) error
	CloseTaskSchedule(ctx context.Context, taskKey string) error
	WaitTaskSchedule(ctx context.Context) (task *Task, err error)
	ReadTask(ctx context.Context, taskKey string) (*Task, error)
	TaskSummery(ctx context.Context) (*Summery, error)
	NewExecID(ctx context.Context, taskKey string) (seq int32, err error)
}

type Context struct {
	context.Context
	manager   *Manager
	taskKey    string
	execution *Execution
}

func (cb Context) Execution() *Execution {
	return cb.execution
}

func (cb Context) TaskKey() string {
	return cb.taskKey
}

func (cb Context) Callback(ctx context.Context, res ExecResult) error {
	err := cb.manager.TaskCallback(ctx, cb.taskKey, cb.execution, res)
	return err
}

type Manager struct {
	//taskSessionTimeOut time.Duration
	pool               *promise.Pool
	executor           Executor
	scheduler          Scheduler
	maxRedundancy time.Duration
}

func NewManager(scheduler Scheduler, executor Executor, maxRedundancy time.Duration) *Manager {
	p := promise.NewPool(8)
	return &Manager{
		executor:           executor,
		pool:               p,
		scheduler:          scheduler,
		maxRedundancy:      maxRedundancy,
	}
}

func (svc *Manager) ApplyNewTask(ctx context.Context, desc Desc) error {
	if desc.ExecDesc.RemainExecCount <= 0 {
		desc.ExecDesc.RemainExecCount = 1
	}

	task := Task{
		Key: desc.TaskKey,
		Info: Info{
			Tags: desc.Tags,
			ExecConfig: desc.ExecDesc,
			CreateTime: time.Now(),
		},
		Schedule: Schedule {
			AwakenTime: desc.ExecDesc.ExpectStartTime,
			Priority:   desc.ExecDesc.Priority,
		},
	}

	err := svc.scheduler.ScheduleTask(ctx, task)
	if err != nil {
		return err
	}
	return err
}

func (svc *Manager) GetTaskSummary(ctx context.Context) (*Summery, error) {
	sum, err := svc.scheduler.TaskSummery(ctx)
	if err != nil {
		return nil, err
	}
	return sum, nil
}


func (svc *Manager) TaskCallback(ctx context.Context, taskKey string, exec *Execution, result ExecResult) error {
	t, err := svc.scheduler.ReadTask(ctx, taskKey)
	if err != nil {
		return err
	}

	now := time.Now()

	if t.Execution.Seq(now) < exec.Seq(now) {
		return nil
	}

	if exec == nil {
		return nil
	}

	exec.End(result, time.Now())

	t.Schedule = Schedule{
		AwakenTime: now,
		Priority:   t.Info.ExecConfig.Priority,
	}

	t.Execution = exec
	err = svc.scheduler.ScheduleTask(ctx, *t)
	return err
}

func (svc *Manager) newTaskExecution(ctx context.Context, task *Task) error {
	maxDuration := task.Info.ExecConfig.MaxExecDuration
	if maxDuration == 0 {
		maxDuration = svc.maxRedundancy
	}
	task.Info.ExecConfig.MaxExecDuration = maxDuration

	if task.Info.ExecConfig.ExpectStartTime.IsZero() {
		task.Info.ExecConfig.ExpectStartTime = time.Now()
	}

	task.Schedule.AwakenTime = task.Info.ExecConfig.ExpectStartTime
	execSeqID, err := svc.scheduler.NewExecID(ctx, task.Key)
	if err != nil {
		return err
	}

	task.NewExec(execSeqID)
	err = svc.scheduler.ScheduleTask(ctx, *task)
	return err
}

func (svc *Manager) getRandomRedundancy() time.Duration {
	return time.Duration(rand.Int63n(int64(svc.maxRedundancy)))
}

func (svc *Manager) getExecutingAwakeTime(exec *Execution) time.Time {
	return exec.Config.ExpectStartTime.Add(exec.Config.MaxExecDuration).Add(svc.getRandomRedundancy())
}

func (svc *Manager) processTask(ctx context.Context, task *Task) error {
	var (
		err error
	)

	if task == nil {
		return nil
	}

	if task.Execution == nil {
		err = svc.newTaskExecution(ctx, task)
		if err != nil {
			return err
		}
	}

	exec := task.Execution
	t := time.Now()

	switch  {
	case exec.ReadyToStart(t):
		svc.doExec(ctx, task, exec)
	case exec.WaitingStart(t):
	case exec.Executing(t):
		if exec.OverExecTime(t) {
			err = svc.newTaskExecution(ctx, task)
		}
	case exec.Ended(t):
		if exec.CanRetry() {
			task.Info.ExecConfig = exec.Result.NextExec
			err = svc.newTaskExecution(ctx, task)
		} else {
			svc.closeTask(ctx, task)
		}
	}

	if err != nil {
		return err
	}

	return err
}

func (svc *Manager) doExec(ctx context.Context, task *Task, execution *Execution) {
	execution.Start(time.Now())
	task.Schedule.AwakenTime = svc.getExecutingAwakeTime(execution)

	err := svc.scheduler.ScheduleTask(ctx, *task)
	if err != nil {
		return
	}

	p := promise.NewPromise(svc.pool, func(ctx context.Context, req promise.Request) promise.Result {
		err := svc.executor.StartExecution(Context{
			Context:   ctx,
			manager:   svc,
			taskKey:   task.Key,
			execution: execution,
		})
		return promise.Result{
			Err: err,
		}
	})
	p.Start(ctx)
	return
}

func (svc *Manager) closeTask(ctx context.Context, t *Task) {
	if svc.scheduler.CloseTaskSchedule(ctx, t.Key) == nil {
		//TODO close memory

	}
}

func (svc *Manager) doProcess(ctx context.Context, task *Task) error {
	h := hashPool.Get().(hash.Hash32)
	defer hashPool.Put(h)
	h.Reset()
	_, err := h.Write([]byte(task.Key))
	if err != nil {
		return err
	}

	p := promise.NewPromise(svc.pool, func(ctx context.Context, req promise.Request) promise.Result {
		err = svc.processTask(ctx, task)
		return promise.Result{
			Err: err,
		}
	}, promise.PartitionMiddle(true), promise.EventKeyMiddle(int(h.Sum32())))
	if p.Start(ctx) {
		return nil
	}
	return ctx.Err()
}

func (svc *Manager) runScheduler(ctx context.Context) error {
	for ctx.Err() == nil {
		task, err := svc.scheduler.WaitTaskSchedule(ctx)
		if err != nil {
			if err == context.Canceled || err == context.DeadlineExceeded {
				continue
			}
			return err
		}
		err = svc.doProcess(ctx, task)
		if err != nil {
			log.Println("process task err:", err)
			continue
		}
	}
	return ctx.Err()
}

func (svc *Manager) Run(ctx context.Context) error {
	return svc.runScheduler(ctx)
}


var hashPool = sync.Pool{
	New: func() interface{} {
		return crc32.NewIEEE()
	},
}
