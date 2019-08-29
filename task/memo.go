package task

import (
	"context"
	"errors"
	"github.com/feynman-go/workshop/memo"
	"math/rand"
	"sync"
	"time"
)

var _ Scheduler = (*MemoScheduler)(nil)

type schedule struct {
	taskKey         string
	expectStartTime time.Time
	awakenDuration time.Duration
}

type MemoScheduler struct {
	m        *sync.Map
	pairChan chan schedule
	awakenDuration time.Duration
}

func NewMemoScheduler(awakenDuration time.Duration) *MemoScheduler {
	return &MemoScheduler{
		m:        &sync.Map{},
		pairChan: make(chan schedule, 64),
		awakenDuration: awakenDuration,
	}
}

func (scheduler *MemoScheduler) EnableTaskClock(ctx context.Context, taskKey string, expectStartTime time.Time) error {
	pair := schedule{
		taskKey: taskKey,
		expectStartTime: expectStartTime,
		awakenDuration: scheduler.awakenDuration,
	}
	scheduler.m.Store(taskKey, pair)
	time.AfterFunc(time.Now().Sub(expectStartTime), func() {
		scheduler.pairChan <- pair
	})
	return nil
}

func (scheduler *MemoScheduler) DisableTaskClock(ctx context.Context, taskKey string) {
	scheduler.m.Delete(taskKey)
}

func (scheduler *MemoScheduler) WaitTaskAwaken(ctx context.Context) (taskKey string, expectStartTime time.Time, err error) {
	tk := time.NewTicker(time.Second)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			return
		case <- tk.C:
			scheduler.m.Range(func(k, v interface{}) bool {
				s := v.(schedule)
				if s.expectStartTime.Add(s.awakenDuration).Before(time.Now()) {
					select {
					case scheduler.pairChan <- s:
					default:
					}
				}
				return true
			})
		case pair, _ := <-scheduler.pairChan:
			_, ok := scheduler.m.Load(pair.taskKey)
			if ok {
				taskKey = pair.taskKey
				expectStartTime = pair.expectStartTime
			}
			return
		}
	}

}

var _ Repository = (*MemoRepository)(nil)

type MemoRepository struct {
	store       *memo.MemoStore
	m           sync.RWMutex
	statusIndex map[Status]int
}

func NewMemoRepository() *MemoRepository {
	return &MemoRepository{
		store:       memo.NewMemoStore(time.Hour, 10*time.Minute),
		statusIndex: map[Status]int{},
	}
}

func (ms *MemoRepository) Run(ctx context.Context) error {
	var runCtx, cancel = context.WithCancel(ctx)
	defer cancel()

	go ms.store.Run(runCtx)

	tk := time.NewTicker(3 * time.Second)
	for ctx.Err() == nil {
		select {
		case <-runCtx.Done():
			return ctx.Err()
		case <-tk.C:

		}
	}
	return ctx.Err()
}

type taskCtn struct {
	rw   sync.RWMutex
	task *Task
}

func (ctn *taskCtn) getTask() *Task {
	var t = &Task{}
	ctn.rw.RLock()
	defer ctn.rw.RUnlock()
	*t = *ctn.task
	t.Executions = make([]*Execution, 0, len(t.Executions))
	for _, ex := range ctn.task.Executions {
		e := *ex
		t.Executions = append(t.Executions, &e)
	}
	return t
}

func (ctn *taskCtn) occupyTask(session Session, update func(task *Task)) bool {
	ctn.rw.Lock()
	defer ctn.rw.Unlock()

	curSession := ctn.task.Session
	if curSession.SessionID != 0 &&
		curSession.SessionID != session.SessionID &&
		curSession.SessionExpires.After(time.Now()) {
		return false
	}

	ctn.task.Session = session
	update(ctn.task)
	return true
}

func (ctn *taskCtn) updateBySession(sessionID int64, update func(task *Task)) bool {
	ctn.rw.Lock()
	defer ctn.rw.Unlock()

	if ctn.task.Session.SessionID != sessionID {
		return false
	}

	if ctn.task.Session.SessionExpires.Before(time.Now()) {
		return false
	}

	update(ctn.task)
	return true
}

func newMemoCtn(task *Task) *taskCtn {
	return &taskCtn{
		task: task,
	}
}

func (ms *MemoRepository) OccupyTask(ctx context.Context, taskKey string, sessionTimeout time.Duration) (*Task, error) {
	s := Session{
		SessionID:      int64(rand.Uint32()),
		SessionExpires: time.Now().Add(sessionTimeout),
	}
	t := &Task{
		Key: taskKey,
	}

	// load data
	ms.m.Lock()
	v, loaded := ms.store.Load(taskKey, newMemoCtn(t))
	ctn := v.(*taskCtn)
	if !loaded {
		ms.statusIndex[ctn.getTask().Status()]++
	}
	ms.m.Unlock()

	before := ctn.getTask().Status()
	if !ctn.occupyTask(s, func(task *Task) {
		ms.onTaskStatusChange(before, task.Status(), task.Key)
	}) {
		return ctn.getTask(), nil
	}
	return ctn.getTask(), nil
}

func (ms *MemoRepository) ReleaseTaskSession(ctx context.Context, taskKey string, sessionID int64) error {
	v, ok := ms.store.Get(taskKey)
	if !ok {
		return nil
	}

	ctn := v.(*taskCtn)
	ok = ctn.updateBySession(sessionID, func(t *Task) {
		t.Session = Session{}
	})
	if !ok {
		return nil
	}

	ms.m.Lock()
	defer ms.m.Unlock()

	t := ctn.getTask()
	if t.Status() == StatusClosed && t.CloseType == CloseTypeSuccess {
		ms.store.Delete(taskKey)
	}

	return nil
}

func (ms *MemoRepository) UpdateTask(ctx context.Context, task *Task) error {
	v, ok := ms.store.Get(task.Key)
	if !ok {
		return errors.New("task not found")
	}

	ctn := v.(*taskCtn)
	before := ctn.getTask().Status()
	ok = ctn.updateBySession(task.Session.SessionID, func(t *Task) {
		*t = *task
		t.Executions = make([]*Execution, 0, len(t.Executions))
		for _, ex := range task.Executions {
			e := *ex
			t.Executions = append(t.Executions, &e)
		}
		ms.onTaskStatusChange(before, t.Status(), t.Key)
	})

	if !ok {
		return errors.New("session not accept")
	}
	return nil
}

func (ms *MemoRepository) ReadTask(ctx context.Context, taskKey string) (*Task, error) {
	v, ok := ms.store.Get(taskKey)
	if !ok || v == nil {
		return nil, nil
	}
	ctn := v.(*taskCtn)
	t := ctn.getTask()
	return t, nil
}

func (ms *MemoRepository) GetTaskSummary(ctx context.Context) (*Summery, error) {
	m := make(map[Status]int64)
	ms.m.RLock()
	defer ms.m.RUnlock()

	for s, count := range ms.statusIndex {
		m[s] = int64(count)
	}

	return &Summery{
		StatusCount: m,
	}, nil
}

func (ms *MemoRepository) onTaskStatusChange(before, current Status, taskKey string) {
	ms.m.Lock()
	defer ms.m.Unlock()

	if before == current {
		return
	}

	ms.statusIndex[before]--
	ms.statusIndex[current]++
}