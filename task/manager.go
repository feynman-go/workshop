package task

import (
	"context"
	"errors"
	"fmt"
	"github.com/feynman-go/workshop/promise"
	"log"
	"math/rand"
	"time"
)

type Executor interface {
	StartExecution(cb Context) error
}

type FuncExecutor func(cb Context) error

func (fe FuncExecutor) StartExecution(cb Context) error {
	return fe(cb)
}

type Context struct {
	context.Context
	manager   *Manager
	unique    string
	execution Execution
}

func (cb Context) Execution() Execution {
	return cb.execution
}

func (cb Context) TaskKey() string {
	return cb.unique
}

func (cb Context) Callback(ctx context.Context, res ExecResult) error {
	err := cb.manager.TaskCallback(ctx, cb.unique, cb.execution.Session, res)
	return err
}

type Manager struct {
	taskSessionTimeOut time.Duration
	pool               *promise.Pool
	repository         Repository
	executor           Executor
	scheduler          Scheduler
}

func NewManager(repository Repository, scheduler Scheduler, executor Executor) *Manager {
	p := promise.NewPool(4)
	return &Manager{
		taskSessionTimeOut: 2*time.Minute - time.Duration(rand.Int31n(60))*time.Second,
		repository:         repository,
		executor:           executor,
		pool:               p,
		scheduler:          scheduler,
	}
}

func (svc *Manager) ApplyNewTask(ctx context.Context, desc Desc) (*Task, error) {
	t, err := svc.repository.OccupyTask(ctx, desc.Unique, svc.taskSessionTimeOut)
	if err != nil {
		return nil, err
	}

	if desc.Strategy.MaxDuration == 0 {
		desc.Strategy.MaxDuration = time.Second
	}
	if desc.Strategy.MaxRetryTimes == 0 {
		desc.Strategy.MaxRetryTimes = 1
	}

	t.UpdateExecStrategy(desc.Strategy)

	t.Init()

	err = svc.processTask(ctx, t)
	if err != nil {
		return t, err
	}
	return t, nil
}

func (svc *Manager) GetTaskSummary(ctx context.Context) (*Summery, error) {
	sum, err := svc.repository.GetTaskSummary(ctx)
	if err != nil {
		return nil, err
	}
	return sum, nil
}

func (svc *Manager) handleStartTimeOn(ctx context.Context, task *Task) (err error) {
	now := time.Now()

	if _, ok := task.StartCurrentExec(now); !ok {
		return errors.New("not start")
	}

	_, ok := task.StartCurrentExec(now)
	if !ok {
		err = errors.New("start startExec failed")
		return err
	}

	err = svc.repository.UpdateTask(ctx, task)
	if err != nil {
		err = fmt.Errorf("update task err: %v", err)
		return err
	}

	if svc.executor != nil {
		exc, _ := task.CurrentExecution()
		svc.startExec(ctx, task.Unique, exc)
	} else {
		err = svc.closeTask(ctx, task, CloseTypeNoExecutor)
	}
	return err
}

func (svc *Manager) startExec(ctx context.Context, unique string, execution Execution) {
	p := promise.NewPromise(svc.pool, func(ctx context.Context, req promise.Request) promise.Result {
		err := svc.executor.StartExecution(Context{
			Context:   ctx,
			manager:   svc,
			unique:    unique,
			execution: execution,
		})
		return promise.Result{
			Err: err,
		}
	})

	p.Start(ctx)
	return
}

func (svc *Manager) TaskCallback(ctx context.Context, unique string, session Session, result ExecResult) error {
	t, err := svc.repository.ReadSessionTask(ctx, unique, session)
	if err != nil {
		return err
	}

	return svc.processTask(ctx, t)
}

func (svc *Manager) closeTask(ctx context.Context, t *Task, closeType CloseType) error {
	svc.scheduler.DisableTaskClock(ctx, t.Unique)
	t.Close(closeType)
	err := svc.repository.UpdateTask(ctx, t)
	if err != nil {
		defer svc.repository.ReleaseTaskSession(ctx, t.Unique, t.Session.SessionID)
		return err
	}
	if t.Closed() {
		defer svc.repository.ReleaseTaskSession(ctx, t.Unique, t.Session.SessionID)
	}
	return nil
}

func (svc *Manager) planExecutionSchedule(ctx context.Context, t *Task) error {
	er, ok := t.CurrentExecution()
	if !ok {
		return errors.New("can not get current startExec record")
	}
	return svc.scheduler.EnableTaskClock(ctx, t.Unique, er.ExpectStartTime)
}

func (svc *Manager) retryTask(ctx context.Context, t *Task) (bool, error) {
	if !t.CanRetry() {
		return false, nil
	}
	err := svc.readyTask(ctx, t, false)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (svc *Manager) readyTask(ctx context.Context, t *Task, overlap bool) error {
	err := t.ReadyNewExec(overlap, t.Session)
	if err != nil {
		defer svc.repository.ReleaseTaskSession(ctx, t.Unique, t.Session.SessionID)
		log.Println("ready new task:", err)
		return err
	}

	err = svc.repository.UpdateTask(ctx, t)
	if err != nil {
		defer svc.repository.ReleaseTaskSession(ctx, t.Unique, t.Session.SessionID)
		log.Println("update task err:", err)
		return err
	}

	err = svc.planExecutionSchedule(ctx, t)
	if err != nil {
		defer svc.repository.ReleaseTaskSession(ctx, t.Unique, t.Session.SessionID)
		log.Println("release task session:", err)
		return err
	}
	return nil
}

func (svc *Manager) retryTaskOrClose(ctx context.Context, task *Task) (retry bool, err error) {
	retry, err = svc.retryTask(ctx, task)
	if err == nil && !retry {
		err = svc.closeTask(ctx, task, CloseTypeNoMoreRetry)
	}
	return
}

func (svc *Manager) processTask(ctx context.Context, task *Task) error {
	var (
		err error
	)

	if task == nil {
		return nil
	}

	var now = time.Now()
	switch s := task.Status(); s {
	case StatusClosed:
		err = svc.closeTask(ctx, task, task.CloseType)
	case StatusCreated:
		err = svc.closeTask(ctx, task, CloseTypeNotInited)
	case StatusInit:
		_, err = svc.retryTaskOrClose(ctx, task)
	case StatusWaitingExec:
		if task.WaitOvertime(now) {
			_, err = svc.retryTaskOrClose(ctx, task)
		} else if task.ReadyExec(now) {
			err = svc.handleStartTimeOn(ctx, task)
		}
	case StatusExecuting:
		if task.ExecOvertime(now) {
			_, err = svc.retryTaskOrClose(ctx, task)
		}
	case StatusExecuteFinished:
		er, _ := task.CurrentExecution()
		if er.Result.ExecResultType == ExecResultTypeSuccess {
			err = svc.closeTask(ctx, task, CloseTypeSuccess)
		} else {
			_, err = svc.retryTaskOrClose(ctx, task)
		}
	default:
		err = fmt.Errorf("bad task status %v", s.String())
	}

	if err != nil || task.Closed() {
		defer svc.repository.ReleaseTaskSession(ctx, task.Unique, task.Session.SessionID)
		return err
	}

	return nil
}

func (svc *Manager) runSupervisor(ctx context.Context) error {
	for ctx.Err() == nil {
		taskKey, _, err := svc.scheduler.WaitTaskAwaken(ctx)
		if err != nil {
			if err == context.Canceled || err == context.DeadlineExceeded {
				continue
			}
			return err
		}
		task, err := svc.repository.OccupyTask(ctx, taskKey, svc.taskSessionTimeOut)
		if err != nil {
			log.Println("repository OccupyTask err:", err)
			continue
		}
		err = svc.processTask(ctx, task)
		if err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (svc *Manager) Run(ctx context.Context) error {
	return svc.runSupervisor(ctx)
}

type Scheduler interface {
	EnableTaskClock(ctx context.Context, taskKey string, expectStartTime time.Time) error
	DisableTaskClock(ctx context.Context, taskKey string)

	WaitTaskAwaken(ctx context.Context) (taskKey string, expectStartTime time.Time, err error)
}
