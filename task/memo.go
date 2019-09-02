package task

import (
	"context"
	"errors"
	"github.com/feynman-go/workshop/memo"
	"strconv"
	"sync"
	"time"
)

var _ Scheduler = (*MemoScheduler)(nil)

type inner struct {
	rw sync.RWMutex
	task Task
	timer *time.Timer
}

func (in *inner) updateTimer(timer *time.Timer) {
	if in.timer != nil {
		in.timer.Stop()
	}
	in.timer = timer
}

func (in *inner) GetTask() *Task {
	in.rw.RLock()
	defer in.rw.RUnlock()

	task := new(Task)
	*task = in.task
	return task
}

type MemoScheduler struct {
	rw sync.RWMutex
	tasks map[string]*inner
	taskChan chan string
	awakenDuration time.Duration
}

func NewMemoScheduler(awakenDuration time.Duration) *MemoScheduler {
	return &MemoScheduler{
		tasks: map[string]*inner{},
		taskChan: make(chan string, 64),
		awakenDuration: awakenDuration,
	}
}

func (scheduler *MemoScheduler) ScheduleTask(ctx context.Context, taskKey string, info Info, schedule Schedule) error {

	scheduler.rw.Lock()
	in, ext := scheduler.tasks[taskKey]
	if !ext {
		in = &inner{}
		scheduler.tasks[taskKey] = in
	}
	in.task = Task{
		Key: taskKey,
		Schedule: schedule,
		Info: info,
	}
	awakeTime := in.task.Schedule.ExpectTime
	scheduler.rw.Unlock()

	scheduler.schedule(in, awakeTime)
	return nil
}

func (scheduler *MemoScheduler) schedule(in *inner, awakeTime time.Time) {
	in.rw.Lock()
	defer in.rw.Unlock()

	taskKey := in.task.Key
	laterAwakeTime := in.task.Schedule.ExpectTime.Add(scheduler.awakenDuration)

	timer := time.AfterFunc(awakeTime.Sub(time.Now()), func() {
		scheduler.rw.RLock()
		in, ext := scheduler.tasks[taskKey]
		scheduler.rw.RUnlock()

		if ext {
			scheduler.schedule(in, laterAwakeTime)
			scheduler.taskChan <- in.task.Key
		}
	})

	in.updateTimer(timer)
}

func (scheduler *MemoScheduler) CloseTaskSchedule(ctx context.Context, taskKey string) error {
	scheduler.rw.Lock()
	defer scheduler.rw.Unlock()

	defer delete(scheduler.tasks, taskKey)

	in, ok := scheduler.tasks[taskKey]
	if !ok {
		return nil
	}

	in.rw.Lock()
	defer in.rw.Unlock()
	// stop timer
	in.updateTimer(nil)
	return nil
}

func (scheduler *MemoScheduler) WaitTaskSchedule(ctx context.Context) (task *Task, err error) {
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case taskKey, _ := <-scheduler.taskChan:
		scheduler.rw.RLock()
		in, ok := scheduler.tasks[taskKey]
		scheduler.rw.RUnlock()
		if ok {
			task = in.GetTask()
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
		return in.GetTask(), nil
	}
}

func (scheduler *MemoScheduler) TaskSummery(ctx context.Context) (*Summery, error) {
	var count = 0
	scheduler.rw.RLock()
	count = len(scheduler.tasks)
	scheduler.rw.RUnlock()
	return &Summery{
		StatusCount: map[StatusCode] int64{
			StatusExecuting: int64(count),
		},
	}, nil
}

var _ ExecutionRepository = (*MemoExecRepository)(nil)

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
}

var taskListPool = &sync.Pool{
	New: func() interface{} {
		return []string{}
	},
}