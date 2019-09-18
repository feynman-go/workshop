package task

import (
	"context"
	"github.com/feynman-go/workshop/promise"
	"github.com/feynman-go/workshop/syncrun"
	"github.com/feynman-go/workshop/syncrun/prob"
	"github.com/pkg/errors"
	"hash"
	"hash/crc32"
	"log"
	"sync"
	"time"
)

type Executor interface {
	Execute(ctx Context, res *Result)
}

type FuncExecutor func(cb Context, res *Result)

func (fe FuncExecutor) Execute(cb Context, res *Result)  {
	fe(cb, res)
}

type Awaken struct {
	Task       Task
	OverLapped *Task
}

type Scheduler interface {
	ScheduleTask(ctx context.Context, task Task, overlap bool) (Task, error)
	CloseTaskSchedule(ctx context.Context, task Task) error
	WaitTaskAwaken(ctx context.Context) (awaken Awaken, err error)
	ReadTask(ctx context.Context, taskKey string) (*Task, error)
	NewStageID(ctx context.Context, taskKey string) (stageID int64, err error)
	Close(ctx context.Context) error
}

type Context struct {
	context.Context
	manager   *Manager
	task    Task
}

func (cb Context) Task() Task {
	return cb.task
}

func (cb Context) KeepLive(ctx context.Context) error {
	err := cb.manager.KeepLive(ctx, cb.task)
	return err
}

type Manager struct {
	pool      *promise.Pool
	executor  Executor
	scheduler Scheduler
	ps        *promiseStore
	pb        *prob.Prob
	option    ManagerOption
}

func NewManager(scheduler Scheduler, executor Executor, opt ManagerOption) *Manager {
	opt = opt.CompleteWith(DefaultManagerOption())
	p := promise.NewPool(opt.MaxBusterTask)
	ret := &Manager{
		executor:           executor,
		pool:               p,
		scheduler:          scheduler,
		option: opt,
		ps: &promiseStore {
			m: &sync.Map{},
		},
	}
	ret.start(context.Background())
	return ret
}

func (svc *Manager) ApplyNewTask(ctx context.Context, taskKey string, option ...Option) error {
	if taskKey == "" {
		return errors.New("empty task key")
	}

	opt := svc.option.Option()
	for _, o := range option {
		opt = opt.Merge(o)
	}

	stageID, err := svc.scheduler.NewStageID(ctx, taskKey)
	if err != nil {
		return err
	}

	var (
		execOption = opt.Exec
		tags []string
		overlap bool
	)

	if opt.Tags != nil {
		tags = *opt.Tags
	}
	if opt.Overlap != nil {
		overlap = *opt.Overlap
	}
	task := Task {
		Key:   taskKey,
		Stage: stageID,
		Info: Info{
			Tags:       tags,
			ExecOption: execOption,
			CreateTime: time.Now(),
		},
		Schedule: execOption.GetExpectStartSchedule(),
	}

	_, err = svc.scheduler.ScheduleTask(ctx, task, overlap)
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
	var now  =	time.Now()

	t.Schedule = Schedule{
		AwakenTime: now,
		CompensateDuration: svc.option.WaitCloseDuration * 2,
	}
	t.Execution.End(Result{}, now)
	_, err = svc.scheduler.ScheduleTask(ctx, *t, true)
	return err
}

func (svc *Manager) TaskCallback(ctx context.Context, task Task, result Result) error {
	t, err := svc.scheduler.ReadTask(ctx, task.Key)
	if err != nil {
		return err
	}

	if t.Stage != task.Stage || t.Status() != statusExecuting { // check version
		return nil
	}

	exec := &t.Execution

	exec.End(result, time.Now())

	t.Schedule = Schedule{
		AwakenTime:         time.Now(),
		CompensateDuration: svc.option.WaitCloseDuration * 2,
	}

	t.Info.StartCount = 0
	_, err = svc.scheduler.ScheduleTask(ctx, *t, false)
	return err
}

func (svc *Manager) KeepLive(ctx context.Context, task Task) error {
	t, err := svc.scheduler.ReadTask(ctx, task.Key)
	if err != nil {
		return err
	}

	if t.Stage != task.Stage || t.Status() != statusExecuting { // check version
		return nil
	}

	exec := &t.Execution
	exec.LastKeepLive = time.Now()

	t.Schedule = exec.Config.GetExecutingSchedule()
	_, err = svc.scheduler.ScheduleTask(ctx, *t, false)
	return err
}


func (svc *Manager) newTaskExecution(ctx context.Context, task *Task) (ok bool, err error) {
	// reconfig by manager option
	if task.Info.ExecOption.MaxRecover != nil && *task.Info.ExecOption.MaxRecover > 0 {
		maxRestart := *task.Info.ExecOption.MaxRecover
		if maxRestart < task.Info.StartCount {
			return false, nil
		}
	}

	task.Schedule = task.Info.ExecOption.GetExpectStartSchedule()
	stageID, err := svc.scheduler.NewStageID(ctx, task.Key)
	if err != nil {
		return false, err
	}

	task.NewExec(stageID, task.Info.ExecOption)
	_, err = svc.scheduler.ScheduleTask(ctx, *task, false)
	return true, err
}

//func (svc *Manager) getRandomRedundancy() time.Duration {
//	return time.Duration(rand.Int63n(int64(svc.option.WaitCloseDuration)))
//}

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
		ok bool
	)

	if task == nil {
		return nil
	}

	if !task.Execution.Available {
		ok, err = svc.newTaskExecution(ctx, task)
		if err != nil {
			return err
		}
		if !ok {
			svc.closeTask(ctx, task)
		}
		return nil
	}

	exec := task.Execution
	t := time.Now()

	switch  {
	case exec.ReadyToStart():
		svc.doExec(ctx, task)
	case exec.WaitingStart():
		_, err = svc.scheduler.ScheduleTask(ctx, *task, false)
	case exec.Executing():
		if exec.OverExecTime(t) || exec.IsDead(t) {
			if task.Info.CanRestart() {
				ok, err = svc.newTaskExecution(ctx, task)
				if err == nil && !ok {
					svc.closeTask(ctx, task)
				}
			} else {
				svc.closeTask(ctx, task)
			}
		}
	case exec.Ended():
		if exec.Result.Continue {
			task.Info.ExecOption = task.Info.ExecOption.Merge(exec.Result.NextExec)
			ok, err = svc.newTaskExecution(ctx, task)
			if err == nil && !ok {
				svc.closeTask(ctx, task)
			}
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

	task.Schedule = task.Execution.Config.GetExecutingSchedule()
	task.Info.StartCount++
	task.Info.ExecCount++
	t, err := svc.scheduler.ScheduleTask(ctx, *task, false)
	if err != nil {
		return
	}

	// todo to imply
	p := promise.NewPromise(svc.pool, func(ctx context.Context, req promise.Request) promise.Result {
		var res = &Result{}
		res.Finish()

		svc.executor.Execute(Context{
			Context:   ctx,
			manager:   svc,
			task:   t,
		}, res)
		err = svc.TaskCallback(ctx, t, *res)
		return promise.Result{
			Err: err,
		}
	})
	svc.ps.setPromise(ctx, task.Key, p, svc.option.WaitCloseDuration)
	return
}

func (svc *Manager) closeTask(ctx context.Context, t *Task) {
	if svc.scheduler.CloseTaskSchedule(ctx, *t) == nil {
		//TODO close memory
		runCtx, _ := context.WithTimeout(ctx, svc.option.WaitCloseDuration)
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

	svc.ps.setPromise(ctx, awaken.Task.Key, p, svc.option.WaitCloseDuration)
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

/*func (svc *Manager) Run(ctx context.Context) error {
	return svc.runScheduler(ctx)
}*/

func (svc *Manager) Close(ctx context.Context) error {
	syncrun.Run(ctx, func(ctx context.Context) {
		svc.pb.Stop()
		select {
		case <- ctx.Done():
		case <- svc.pb.Stopped():
		}
	}, func(ctx context.Context) {
		svc.scheduler.Close(ctx)
	})
	return nil
}

func (svc *Manager) start(ctx context.Context) {
	pb := prob.New(syncrun.FuncWithRandomStart(func(ctx context.Context) bool {
		err := svc.runScheduler(ctx)
		if err != nil {
			log.Println("run scheduler err:", err)
		}
		return true
	}, syncrun.RandRestart(time.Second, 3 * time.Second)))
	pb.Start()
	svc.pb = pb

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
		return &promiseWrap {

		}
	},
}

var hashPool = sync.Pool{
	New: func() interface{} {
		return crc32.NewIEEE()
	},
}
