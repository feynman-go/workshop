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

type ExecutionRepository interface {
	NewExecutionID(ctx context.Context, taskKey string) (int64, error)
	FindExecution(ctx context.Context, taskKey string, executionID int64) (*Execution, error)
	UpdateExecution(ctx context.Context, execution *Execution) error
}

type Scheduler interface {
	ScheduleTask(ctx context.Context, taskKey string, info Info, schedule Schedule) error
	CloseTaskSchedule(ctx context.Context, taskKey string) error
	WaitTaskSchedule(ctx context.Context) (task *Task, err error)
	ReadTask(ctx context.Context, taskKey string) (*Task, error)
	TaskSummery(ctx context.Context) (*Summery, error)
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
	execRep ExecutionRepository
	executor           Executor
	scheduler          Scheduler
	maxRedundancy time.Duration
}

func NewManager(repository ExecutionRepository, scheduler Scheduler, executor Executor, maxRedundancy time.Duration) *Manager {
	p := promise.NewPool(8)
	return &Manager{
		executor:           executor,
		pool:               p,
		scheduler:          scheduler,
		execRep: 			repository,
		maxRedundancy:      maxRedundancy,
	}
}

func (svc *Manager) ApplyNewTask(ctx context.Context, desc Desc) error {
	now := time.Now()
	err := svc.scheduler.ScheduleTask(ctx, desc.TaskKey, Info{
		Tags: desc.Tags,
		MaxExecCount: desc.MaxExecCount,
		MaxExecDuration: desc.MaxExecDuration,
	}, Schedule{
		AcceptTime: now,
		ExpectTime: desc.ExpectStartTime,
		Priority: desc.Priority,
	})

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

	exec, err = svc.execRep.FindExecution(ctx, taskKey, exec.ID)
	if err != nil {
		return err
	}

	if exec == nil {
		return nil
	}

	exec.End(result, time.Now())

	if t.Info.ExecutionID != exec.ID {
	} else {
		now := time.Now()
		err = svc.scheduler.ScheduleTask(ctx, taskKey, t.Info, Schedule{
			AcceptTime: now,
			ExpectTime: now,
			Priority: t.Schedule.Priority,
		})
	}

	return err
}

func (svc *Manager) newTaskExecution(ctx context.Context, task *Task, execDesc ExecDesc) (*Execution, error) {
	id, err := svc.execRep.NewExecutionID(ctx, task.Key)
	if err != nil {
		return nil, err
	}

	maxDuration := execDesc.MaxExecDuration
	if maxDuration == 0 {
		maxDuration = task.Info.MaxExecDuration
	}

	if execDesc.ExpectRetryTime.IsZero() {
		execDesc.ExpectRetryTime = time.Now()
	}

	var now = time.Now()

	task.Schedule.ExpectTime = execDesc.ExpectRetryTime
	task.Schedule.AcceptTime = now
	task.Info.ExecutionID = id

	exec := &Execution{
		ID: id,
		TaskID: task.Key,
		ExpectStartTime: execDesc.ExpectRetryTime,
		CreateTime: time.Now(),
		MaxExecDuration: maxDuration,
	}

	err = svc.execRep.UpdateExecution(ctx, exec)
	if err != nil {
		return nil, err
	}

	err = svc.scheduler.ScheduleTask(ctx, task.Key, task.Info, task.Schedule)
	return exec, err
}

func (svc *Manager) execCanRetry(task *Task, exec *Execution) bool {
	if task.Info.MaxExecCount <= task.Info.ExecCount {
		return false
	}
	return exec.CanRetry()
}

func (svc *Manager) getRandomRedundancy() time.Duration {
	return time.Duration(rand.Int63n(int64(svc.maxRedundancy)))
}

func (svc *Manager) getExecutingAwakeTime(exec *Execution) time.Time {
	return exec.ExpectStartTime.Add(exec.MaxExecDuration).Add(svc.getRandomRedundancy())
}

func (svc *Manager) processTask(ctx context.Context, task *Task) error {
	var (
		err error
	)

	if task == nil {
		return nil
	}

	var exec *Execution
	var t = time.Now()

	if task.Info.ExecutionID == 0 {
		exec, err = svc.newTaskExecution(ctx, task, ExecDesc{
			ExpectRetryTime: task.Schedule.ExpectTime,
		})
	} else {
		exec, err = svc.execRep.FindExecution(ctx, task.Key, task.Info.ExecutionID)
	}

	if err != nil {
		return err
	}

	switch  {
	case exec.ReadyToStart(t):
		svc.doExec(ctx, task, exec)
	case exec.WaitingStart(t):
	case exec.Executing(t):
		if exec.OverExecTime(t) {
			_, err = svc.newTaskExecution(ctx, task, ExecDesc{
				ExpectRetryTime: svc.getExecutingAwakeTime(exec),
			})
		}
	case exec.Ended(t):
		if svc.execCanRetry(task, exec) {
			_, err = svc.newTaskExecution(ctx, task, *exec.Result.RetryInfo)
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
	task.Schedule.ExpectTime = svc.getExecutingAwakeTime(execution)
	task.Info.MaxExecCount ++
	task.Info.Executing = true

	err := svc.scheduler.ScheduleTask(ctx, task.Key, task.Info, task.Schedule)
	if err != nil {
		return
	}

	err = svc.execRep.UpdateExecution(ctx, execution)
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
