package task

import (
	"context"
	"errors"
	"sync"
	"time"
)

var _ Scheduler = (*MemoScheduler)(nil)

type inner struct {
	rw sync.RWMutex
	task Task
	overlap *Task
	timer *time.Timer
}

func (in *inner) UpdateTask(task Task, overlap bool) (ok bool) {
	in.rw.Lock()
	defer in.rw.Unlock()

	if in.task.Stage > task.Stage {
		return false
	}

	if in.task.Stage == task.Stage && in.task.Status() > task.Status() {
		return false
	}

	var cur = in.task
	in.overlap = nil
	if overlap {
		in.overlap = &cur
	}

	in.task = task
	in.task.Stage++
	if in.timer != nil {
		in.timer.Stop()
		in.timer = nil
	}
	return true
}

func (in *inner) UpdateTimer(timer *time.Timer) {
	in.rw.Lock()
	defer in.rw.Unlock()

	if in.timer != nil {
		in.timer.Stop()
	}
	in.timer = timer
}

func (in *inner) GetAwaken() Awaken {
	in.rw.RLock()
	defer in.rw.RUnlock()

	awk := Awaken {
		Task: in.task,
	}
	if in.overlap != nil {
		olp := new(Task)
		*olp = *in.overlap
		awk.OverLapped = olp
	}
	return awk
}

type MemoScheduler struct {
	rw                      sync.RWMutex
	tasks                   map[string]*inner
	taskChan                chan string
	defaultKeepLiveDuration time.Duration
}

func NewMemoScheduler(defaultKeepLiveDuration time.Duration) *MemoScheduler {
	return &MemoScheduler{
		tasks:                   map[string]*inner{},
		taskChan:                make(chan string, 64),
		defaultKeepLiveDuration: defaultKeepLiveDuration,
	}
}

func (scheduler *MemoScheduler) Close(ctx context.Context) error {
	scheduler.rw.RLock()
	defer scheduler.rw.RUnlock()
	for _, t := range scheduler.tasks {
		t.UpdateTimer(nil)
	}
	return nil
}

func (scheduler *MemoScheduler) ScheduleTask(ctx context.Context, task Task, overlap bool) (Task, error) {
	scheduler.rw.Lock()
	in, ext := scheduler.tasks[task.Key]
	var awakeTime = task.Schedule.AwakenTime
	if !ext {
		in = &inner {
			task: task,
		}
		scheduler.tasks[task.Key] = in
	} else {
		if !in.UpdateTask(task, overlap) {
			scheduler.rw.Unlock()
			return task, errors.New("schedule task err")
		}
		if overlap {
			awakeTime = time.Now()
		} else {
			awakeTime = in.GetAwaken().Task.Schedule.AwakenTime
		}

	}
	scheduler.rw.Unlock()
	scheduler.schedule(in, awakeTime, in.GetAwaken().Task.Schedule.CompensateDuration)
	return in.GetAwaken().Task, nil
}

func (scheduler *MemoScheduler) schedule(in *inner, awakeTime time.Time, keepLive time.Duration) {
	taskKey := in.GetAwaken().Task.Key
	if keepLive == 0 {
		keepLive = scheduler.defaultKeepLiveDuration
	}

	laterAwakeTime := awakeTime.Add(keepLive)
	if keepLive == 0 {
		laterAwakeTime = awakeTime.Add(scheduler.defaultKeepLiveDuration)
	}

	timer := time.AfterFunc(awakeTime.Sub(time.Now()), func() {
		scheduler.rw.RLock()
		in, ext := scheduler.tasks[taskKey]
		scheduler.rw.RUnlock()

		if ext {
			scheduler.schedule(in, laterAwakeTime, keepLive)
			scheduler.taskChan <- in.task.Key
		}
	})

	in.UpdateTimer(timer)
}

func (scheduler *MemoScheduler) CloseTaskSchedule(ctx context.Context, task Task) error {
	scheduler.rw.Lock()
	defer scheduler.rw.Unlock()

	defer delete(scheduler.tasks, task.Key)

	in, ok := scheduler.tasks[task.Key]
	if !ok {
		return nil
	}

	if !in.UpdateTask(task, false) {
		return nil
	}
	in.UpdateTimer(nil)
	return nil
}

func (scheduler *MemoScheduler) WaitTaskAwaken(ctx context.Context) (awaken Awaken, err error) {
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case taskKey, _ := <-scheduler.taskChan:
		scheduler.rw.RLock()
		in, ok := scheduler.tasks[taskKey]
		scheduler.rw.RUnlock()
		if ok {
			awaken = in.GetAwaken()
		}
	}
	return
}

func (scheduler *MemoScheduler) ReadTask(ctx context.Context, taskKey string) (*Task, error) {
	scheduler.rw.RLock()
	in, ok := scheduler.tasks[taskKey]
	scheduler.rw.RUnlock()
	if !ok {
		return nil, nil
	} else {
		task := in.GetAwaken().Task
		return &task, nil
	}
}

func (scheduler *MemoScheduler) TaskSummery(ctx context.Context) (*Summery, error) {
	var count = 0
	scheduler.rw.RLock()
	count = len(scheduler.tasks)
	scheduler.rw.RUnlock()
	return &Summery{
		StatusCount: map[StatusCode] int64{
			statusExecuting: int64(count),
		},
	}, nil
}

func (scheduler *MemoScheduler) NewStageID(ctx context.Context, taskKey string) (seq int64, err error) {
	scheduler.rw.RLock()
	in, ext := scheduler.tasks[taskKey]
	scheduler.rw.RUnlock()
	if !ext {
		return 1, nil
	}
	t := in.GetAwaken().Task
	return t.Stage + 1, nil
}

/*var _ ExecutionRepository = (*MemoExecRepository)(nil)

type MemoExecRepository struct {
	store       *memo.MemoStore
	taskIndex   *memo.MemoStore
	m           sync.RWMutex
	statusIndex map[StatusCode]int
}

func NewMemoRepository() *MemoExecRepository {
	return &MemoExecRepository{
		store:       memo.NewMemoStore(time.Hour, 10*time.Minute),
		statusIndex: map[StatusCode]int{},
	}
}

func (ms *MemoExecRepository) NewExecutionID(ctx context.Context, taskKey string) (int64, error) {
	return time.Now().UnixNano(), nil
}

func (ms *MemoExecRepository) FindExecution(ctx context.Context, taskKey string, executionID int64) (*Execution, error) {
	d, ok := ms.store.Get(taskKey + ":" + strconv.FormatInt(executionID, 16))
	if !ok {
		return nil, nil
	}
	ex, ok := d.(*Execution)
	if !ok {
		return nil, errors.New("not execution")
	}
	return ex, nil
}

func (ms *MemoExecRepository) UpdateExecution(ctx context.Context, execution *Execution) error {
	id := execution.TaskID + ":" + strconv.FormatInt(execution.ID, 16)
	return ms.store.Set(id, execution)
}

func (ms *MemoExecRepository) Run(ctx context.Context) error {
	var runCtx, cancel = context.WithCancel(ctx)
	defer cancel()

	return ms.store.Run(runCtx)
}*/

//var taskListPool = &sync.Pool{
//	New: func() interface{} {
//		return []string{}
//	},
//}