package task

import (
	"errors"
	"math/rand"
	"time"
)

var ErrOverMaxRetry = errors.New("over max retry")

var ErrExecuting = errors.New("is executing")

const (
	StatusCreated          Status = 0
	StatusInit             Status = 1 << 10
	StatusWaitingExec      Status = 2 << 10
	StatusExecuting        Status = 3 << 10
	StatusExecuteFinished  Status = 4 << 10
	StatusClosed           Status = 5 << 10
	StatusUnknown          Status = 128 << 10
)

type CloseType int

const (
	CloseTypeSuccess CloseType = 0
	CloseTypeNoMoreRetry CloseType = 1
	CloseTypeCanceled CloseType = 2
	CloseTypeNotInited CloseType = 3
	CloseTypeUnexpect CloseType = 4
	CloseTypeNoExecutor CloseType = 2
)

type Status int

func (status Status) String() string {
	switch status {
	case StatusCreated:
		return "created"
	case StatusInit:
		return "init"
	case StatusWaitingExec:
		return "waiting exec"
	case StatusExecuting:
		return "executing"
	case StatusExecuteFinished:
		return "exec finished"
	case StatusClosed:
		return "closed"
	default:
		return "unknown"
	}
}

const (
	ExecResultTypeUnfinished ResultType = 0
	ExecResultTypeCancel ResultType = 1
	ExecResultTypeErr ResultType = 2
	ExecResultTypeTimeOut ResultType = 3
	ExecResultTypeSuccess ResultType = 4
)

type ResultType int

type Task struct {
	Unique          string        `bson:"_id"`
	ExecStrategy    ExecStrategy  `bson:"strategy"`
	CreateTime      time.Time     `bson:"create_time"`
	LiveTime        time.Time     `bson:"live_time"`
	EndTime         time.Time     `bson:"end_time"`
	HandlerTimeout  time.Duration `bson:"time_out"`
	HandlerID       int64         `bson:"handler_id"`
	HandlerSequence int64         `bson:"handler_seq"`
	//Status          Status        `bson:"status"`
	Executions      []Execution   `bson:"execs"`
	Session         Session       `bson:"session"`
	CloseType       CloseType     `bson:"closeType"`
}

type Session struct {
	SessionID int64
	SessionExpires time.Time
}

func TaskFromDesc(desc Desc) *Task {
	t := &Task{
		Unique: desc.Unique,
		CreateTime: time.Now(),
	}
	t.UpdateExecStrategy(desc.Strategy)
	return t
}

func (task *Task) Init() {
	if task.LiveTime.IsZero() {
		task.LiveTime = time.Now()
	}
}

func (task *Task) ReadyNewExec(force bool, session Session) (err error) {
	if task.LiveTime.IsZero() {
		task.LiveTime = time.Now()
	}

	er := Execution{}
	er.ExecSeq = len(task.Executions)
	if !task.CanRetry() {
		return ErrOverMaxRetry
	}

	if task.OnExecution() && !force {
		return ErrExecuting
	} else {
		task.CancelExec("force startExec new")
	}

	now := time.Now()
	_, ext := task.CurrentExecution()

	er.CreateTime = now
	if !ext {
		er.ExpectStartTime = task.ExecStrategy.ExpectStartTime
	} else {
		maxWait := int32((task.ExecStrategy.MaxRetryWait) / time.Millisecond)
		minWait := int32((task.ExecStrategy.MinRetryWait) / time.Millisecond)
		waitTime := time.Duration(minWait + rand.Int31n(maxWait - minWait)) * time.Millisecond
		if waitTime >= 0 {
			er.ExpectStartTime = now.Add(waitTime)
		}
	}
	if er.ExpectStartTime.IsZero() {
		er.ExpectStartTime = now
	}
	er.Session = session
	task.Executions = append(task.Executions, er)
	return nil
}

func (task *Task) CanRetry() bool {
	if task.ExecStrategy.MaxRetryTimes >= 0 && len(task.Executions) > task.ExecStrategy.MaxRetryTimes {
		return false
	}
	return true
}

func (task *Task) StartCurrentExec(now time.Time) (Execution, bool) {
	er, ext := task.CurrentExecution()
	if !ext {
		return Execution{}, false
	}

	if !er.ReadyToStart(now) {
		return er, false
	}
	er.StartTime = now
	return er, true
}

func (task *Task) CurrentExecution() (Execution, bool){
	if len(task.Executions) == 0 {
		return Execution{}, false
	}
	return task.Executions[len(task.Executions) - 1], true
}

func (task *Task) WaitOvertime(now time.Time) bool {
	exec, ok := task.CurrentExecution()
	if !ok {
		return false
	}
	return exec.ExpectStartTime.Add(task.ExecStrategy.MaxDuration).Before(now)
}

func (task *Task) ReadyExec(now time.Time) bool {
	exec, ok := task.CurrentExecution()
	if !ok {
		return false
	}
	if exec.ReadyToStart(now) && exec.ExpectStartTime.Add(task.ExecStrategy.MaxDuration).After(now) {
		return true
	}
	return false
}


func (task *Task) ExecOvertime(now time.Time) bool {
	exec, ok := task.CurrentExecution()
	if !ok {
		return false
	}

	if !exec.Started(now) {
		return false
	}

	return exec.StartTime.Add(task.ExecStrategy.MaxDuration).After(now)
}


func (task *Task) Status() Status {
	var status Status
	if task.Closed() {
		status = StatusClosed
		return status
	}

	var now = time.Now()
	if exc, ok := task.CurrentExecution(); ok {
		switch {
		case exc.WaitingStart(now) || exc.ReadyToStart(now):
			status = StatusWaitingExec
		case exc.Started(now):
			status = StatusExecuting
		case exc.Ended(now):
			status = StatusExecuteFinished
		default:
			status = StatusUnknown
		}
	} else if !task.LiveTime.IsZero() {
		status = StatusInit
	} else {
		status = StatusCreated
	}
	return status
}


// 包括等待时间
func (task *Task) OnExecution() bool {
	ec, ok := task.CurrentExecution()
	if !ok {
		return false
	}
	now := time.Now()
	if ec.Started(now) && !ec.Ended(now) {
		return true
	}
	return false
}


func (task *Task) CloseExec(result ExecResult) bool {
	if !task.OnExecution() {
		return false
	}
	er, _ := task.CurrentExecution()
	er.Result.ExecResultType = result.ExecResultType
	er.EndTime = time.Now()
	er.Result.ResultInfo = result.ResultInfo
	return true
}

func (task *Task) UpdateExecStrategy(strategy ExecStrategy) {
	task.ExecStrategy = strategy
}

func (task *Task) CancelExec(info string) bool {
	return task.CloseExec(ExecResult{
		info,ExecResultTypeCancel,
	})
}

func (task *Task) Close(closeType CloseType)  {
	if task.Closed() {
		return
	}
	if task.EndTime.IsZero() {
		task.EndTime = time.Now()
	}
	task.CloseType = closeType
}

func (task *Task) Closed() bool {
	if task.CloseType != 0 {
		return true
	}
	return false
}

func (task *Task) Profile() Profile {
	var ex []Execution
	for _, e := range task.Executions {
		ex = append(ex, e)
	}
	return Profile {
		TaskUnique: task.Unique,
		Status: task.Status(),
		Executions: ex,
	}
}

type Execution struct {
	ExecSeq int `bson:"exec_seq"`
	ExpectStartTime time.Time `bson:"expect_start_time"`
	CreateTime time.Time `bson:"create_time"`
	StartTime time.Time `bson:"start_time,omitempty"`
	EndTime time.Time `bson:"end_time,omitempty"`
	Result ExecResult `bson:"result,omitempty"`
	Session Session
}

func (er Execution) ReadyToStart(t time.Time) bool {
	if er.Started(t) || er.Ended(t) {
		return false
	}
	return er.ExpectStartTime.Before(t) || er.ExpectStartTime.Equal(t)
}

func (er Execution) WaitingStart(t time.Time) bool {
	if er.Started(t) || er.Ended(t) {
		return false
	}
	return er.ExpectStartTime.After(t)
}


func (er Execution) Started(t time.Time) bool {
	if er.StartTime.IsZero() {
		return false
	}
	return er.StartTime.Before(t)
}

func (er Execution) Ended(t time.Time) bool {
	if er.EndTime.IsZero() {
		return false
	}
	return er.EndTime.Before(t)
}

type ExecStrategy struct {
	ExpectStartTime time.Time
	MaxDuration time.Duration
	MaxRetryTimes int
	MaxRetryWait time.Duration
	MinRetryWait time.Duration
}

type Desc struct {
	Unique string
	Strategy ExecStrategy
}

type ExecResult struct {
	ResultInfo string
	ExecResultType ResultType
}

type Summery struct {
	StatusCount map[Status]int64
}

type StatusProfile struct {
	Total    int64     `json:"total"`
	Count    int64     `json:"count"`
	Profiles []Profile `json:"profile"`
}

type Profile struct {
	TaskUnique string
	Status Status
	Executions []Execution
}

