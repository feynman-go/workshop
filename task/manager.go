package task

import (
	"context"
	"github.com/feynman-go/workshop/promise"
	"github.com/pkg/errors"
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

type Awaken struct {
	Task       Task
	OverLapped *Task
}

type Scheduler interface {
	ScheduleTask(ctx context.Context, task Task, overlap bool) (Task, error)
	CloseTaskSchedule(ctx context.Context, taskKey string) error
	WaitTaskAwaken(ctx context.Context) (awaken Awaken, err error)
	ReadTask(ctx context.Context, taskKey string) (*Task, error)
	NextSeqID(ctx context.Context, taskKey string) (seq int64, err error)
}

type Context struct {
	context.Context
	manager   *Manager
	task    Task
	execCount int32
}

func (cb Context) ExecCount() int32 {
	return cb.execCount
}

func (cb Context) Task() Task {
	return cb.task
}

func (cb Context) Callback(ctx context.Context, res ExecResult) error {
	err := cb.manager.TaskCallback(ctx, cb.task, res)
	return err
}

type Manager struct {
	//taskSessionTimeOut time.Duration
	pool               *promise.Pool
	executor           Executor
	scheduler          Scheduler
	maxRedundancy      time.Duration
	ps 				   *promiseStore
}

func NewManager(scheduler Scheduler, executor Executor, maxRedundancy time.Duration) *Manager {
	p := promise.NewPool(8)
	return &Manager{
		executor:           executor,
		pool:               p,
		scheduler:          scheduler,
		maxRedundancy:      maxRedundancy,
		ps: &promiseStore {
			m: &sync.Map{},
		},
	}
}

func (svc *Manager) ApplyNewTask(ctx context.Context, desc Desc) error {
	if desc.TaskKey == "" {
		return errors.New("empty task key")
	}
	if desc.ExecDesc.RemainExecCount <= 0 {
		desc.ExecDesc.RemainExecCount = 1
	}

	nextSeq, err := svc.scheduler.NextSeqID(ctx, desc.TaskKey)
	if err != nil {
		return err
	}

	task := Task {
		Key: desc.TaskKey,
		Seq: nextSeq,
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

	_, err = svc.scheduler.ScheduleTask(ctx, task, desc.Overlap)
	if err != nil {
		return err
	}
	return err
}

func (svc *Manager) CloseTask(ctx context.Context, taskKey string) error {
	t, err := svc.scheduler.ReadTask(ctx, taskKey)
	if err != nil {
		return err
	}
	if t == nil {
		return nil
	}
	t.Execution.End(ExecResult{
		NextExec: ExecConfig{
			ExpectStartTime: time.Now(),
		},
	}, time.Now())
	_, err = svc.scheduler.ScheduleTask(ctx, *t, true)
	return err
}

func (svc *Manager) TaskCallback(ctx context.Context, task Task, result ExecResult) error {
	t, err := svc.scheduler.ReadTask(ctx, task.Key)
	if err != nil {
		return err
	}

	if t.Seq != task.Seq { // check version
		return nil
	}

	exec := &t.Execution

	exec.End(result, time.Now())

	t.Schedule = Schedule{
		AwakenTime: time.Now(),
		Priority:   t.Info.ExecConfig.Priority,
	}

	_, err = svc.scheduler.ScheduleTask(ctx, *t, false)
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
	task.NewExec()
	_, err := svc.scheduler.ScheduleTask(ctx, *task, false)
	return err
}

func (svc *Manager) getRandomRedundancy() time.Duration {
	return time.Duration(rand.Int63n(int64(svc.maxRedundancy)))
}

func (svc *Manager) getExecutingAwakeTime(exec Execution) time.Time {
	return exec.Config.ExpectStartTime.Add(exec.Config.MaxExecDuration).Add(svc.getRandomRedundancy())
}

func (svc *Manager) processOverlappedTask(ctx context.Context, task *Task) error {
	if task == nil {
		return nil
	}
	if !task.Execution.Available {
		return nil
	}
	svc.ps.delete(ctx, task.Key)
	return nil
}

func (svc *Manager) processTask(ctx context.Context, task *Task) error {
	var (
		err error
	)

	if task == nil {
		return nil
	}

	if !task.Execution.Available {
		err = svc.newTaskExecution(ctx, task)
		if err != nil {
			return err
		}
		return nil
	}

	exec := task.Execution
	t := time.Now()

	switch  {
	case exec.ReadyToStart(t):
		svc.doExec(ctx, task)
	case exec.WaitingStart(t):
		_, err = svc.scheduler.ScheduleTask(ctx, *task, false)
	case exec.Executing(t):
		if exec.OverExecTime(t) && task.Info.ExecConfig.RemainExecCount > 0 {
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

func (svc *Manager) doExec(ctx context.Context, task *Task) {
	task.Execution.Start(time.Now())
	task.Schedule.AwakenTime = svc.getExecutingAwakeTime(task.Execution)
	task.Info.ExecConfig.RemainExecCount --
	task.Info.ExecCount ++
	t, err := svc.scheduler.ScheduleTask(ctx, *task, false)
	if err != nil {
		return
	}

	p := promise.NewPromise(svc.pool, func(ctx context.Context, req promise.Request) promise.Result {
		err := svc.executor.StartExecution(Context{
			Context:   ctx,
			manager:   svc,
			task:   t,
			execCount: task.Info.ExecCount,
		})
		return promise.Result{
			Err: err,
		}
	})
	svc.ps.setPromise(ctx, task.Key, p, svc.maxRedundancy)
	return
}

func (svc *Manager) closeTask(ctx context.Context, t *Task) {
	if svc.scheduler.CloseTaskSchedule(ctx, t.Key) == nil {
		//TODO close memory
		runCtx, _ := context.WithTimeout(ctx, svc.maxRedundancy)
		svc.ps.delete(runCtx, t.Key)
	}
}

func (svc *Manager) doProcess(ctx context.Context, awaken Awaken) error {
	h := hashPool.Get().(hash.Hash32)
	defer hashPool.Put(h)
	h.Reset()
	_, err := h.Write([]byte(awaken.Task.Key))
	if err != nil {
		return err
	}

	p := promise.NewPromise(svc.pool, func(ctx context.Context, req promise.Request) promise.Result {
		if awaken.OverLapped != nil {
			err = svc.processOverlappedTask(ctx, awaken.OverLapped)
			if err != nil {
				return promise.Result{
					Err: err,
				}
			}
		}
		err = svc.processTask(ctx, &awaken.Task)
		return promise.Result{
			Err: err,
		}
	}, promise.PartitionMiddle(true), promise.EventKeyMiddle(int(h.Sum32())))

	svc.ps.setPromise(ctx, awaken.Task.Key, p, svc.maxRedundancy)
	return ctx.Err()
}

func (svc *Manager) runScheduler(ctx context.Context) error {
	for ctx.Err() == nil {
		awaken, err := svc.scheduler.WaitTaskAwaken(ctx)
		if err != nil {
			if err == context.Canceled || err == context.DeadlineExceeded {
				continue
			}
			return err
		}
		err = svc.doProcess(ctx, awaken)
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

type promiseStore struct {
	m *sync.Map
}

type promiseWrap struct {
	rw sync.RWMutex
	ps *promise.Promise
	closed bool
}

func (warp *promiseWrap) ReplacePromise(ctx context.Context, ps *promise.Promise, maxWaitTime time.Duration) {
	warp.rw.Lock()
	defer warp.rw.Unlock()

	if warp.closed {
		return
	}

	old := warp.ps
	warp.ps = ps
	if old != nil && !old.IsStarted() && !old.IsClosed() {
		old.Close()
		runCtx, _ := context.WithTimeout(ctx, maxWaitTime)
		old.Wait(runCtx, false)
	}
	if ctx.Err() == nil {
		ps.Start(ctx)
	}
}

func (warp *promiseWrap) close(ctx context.Context) {
	if warp.closed {
		return
	}
	if warp.ps != nil {
		warp.ps.Close()
		warp.ps.Wait(ctx, false)
	}
	warp.closed = true
}

func (store *promiseStore) setPromise(ctx context.Context, taskKey string, p *promise.Promise, maxWaitDuration time.Duration) {
	pw := promiseWrapPool.Get().(*promiseWrap)
	v, loaded := store.m.LoadOrStore(taskKey, pw)

	if loaded {
		promiseWrapPool.Put(pw)
	}

	pw = v.(*promiseWrap)
	pw.ReplacePromise(ctx, p, maxWaitDuration)
}

func (store *promiseStore) delete(ctx context.Context, taskKey string) {
	v, ok := store.m.Load(taskKey)
	if !ok {
		return
	}

	pw := v.(*promiseWrap)

	pw.rw.Lock()
	pw.close(ctx)
	store.m.Delete(taskKey)
	pw.rw.Unlock()
}

var promiseWrapPool = sync.Pool{
	New: func() interface{} {
		return 	&promiseWrap{

		}
	},
}

var hashPool = sync.Pool{
	New: func() interface{} {
		return crc32.NewIEEE()
	},
}
