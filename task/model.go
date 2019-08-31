package task

import (
	"errors"
	"math/rand"
	"time"
)

var ErrOverMaxRetry = errors.New("over max retry")

var ErrExecuting = errors.New("is executing")

const (
	StatusCreated         Status = 0
	StatusInit            Status = 1 << 10
	StatusWaitingExec     Status = 2 << 10
	StatusExecuting       Status = 3 << 10
	StatusExecuteFinished Status = 4 << 10
	StatusClosed          Status = 5 << 10
	StatusUnknown         Status = 128 << 10
)

type CloseType int

const (
	CloseTypeSuccess     CloseType = 0
	CloseTypeNoMoreRetry CloseType = 1
	CloseTypeCanceled    CloseType = 2
	CloseTypeNotInited   CloseType = 3
	CloseTypeUnexpect    CloseType = 4
	CloseTypeNoExecutor  CloseType = 2
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
	ExecResultTypeCancel     ResultType = 1
	ExecResultTypeErr        ResultType = 2
	ExecResultTypeTimeOut    ResultType = 3
	ExecResultTypeSuccess    ResultType = 4
)

type ResultType int

type Task struct {
	Key             string        `bson:"_id"`
	ExecStrategy    ExecStrategy  `bson:"strategy"`
	CreateTime      time.Time     `bson:"create_time"`
	LiveTime        time.Time     `bson:"live_time"`
	EndTime         time.Time     `bson:"end_time"`
	HandlerTimeout  time.Duration `bson:"time_out"`
	HandlerID       int64         `bson:"handler_id"`
	HandlerSequence int64         `bson:"handler_seq"`
	//Status          Status        `bson:"status"`
	Executions []*Execution `bson:"execs"`
	Session    Session     `bson:"session"`
	CloseType  CloseType   `bson:"closeType"`
}

type Session struct {
	SessionID      int64
	SessionExpires time.Time
}

func TaskFromDesc(desc Desc) *Task {
	t := &Task{
		Key:        desc.TaskKey,
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

func (task *Task) ReadyNewExec(forceClose bool) (err error) {
	if task.LiveTime.IsZero() {
		task.LiveTime = time.Now()
	}
	var (
		execSeq = len(task.Executions)
		expectStartTime time.Time
	)

	if !task.CanRetry() {
		return ErrOverMaxRetry
	}

	cur := task.CurrentExecution()
	if task.OnExecution() && !forceClose {
		return ErrExecuting
	} else if cur != nil {
		task.CancelExec("force startExec new", cur.Seq())
	}


	now := time.Now()
	if cur == nil {
		expectStartTime = task.ExecStrategy.ExpectStartTime
	} else {
		maxWait := int32((task.ExecStrategy.MaxRetryWait) / time.Millisecond)
		minWait := int32((task.ExecStrategy.MinRetryWait) / time.Millisecond)
		delta := maxWait-minWait
		if delta != 0 {
			waitTime := time.Duration(minWait+rand.Int31n(delta)) * time.Millisecond
			if waitTime >= 0 {
				expectStartTime = now.Add(waitTime)
			}
		}
	}
	if expectStartTime.IsZero() {
		expectStartTime = now
	}

	er := NewExecution(execSeq, expectStartTime)
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
	er := task.CurrentExecution()
	if er == nil {
		return Execution{}, false
	}

	if !er.ReadyToStart(now) {
		return *er, false
	}

	er.startTime = now
	return *er, true
}

func (task *Task) CurrentExecution() *Execution {
	if len(task.Executions) == 0 {
		return nil
	}
	return task.Executions[len(task.Executions)-1]
}

func (task *Task) WaitOvertime(now time.Time) bool {
	exec := task.CurrentExecution()
	if exec == nil {
		return false
	}
	return exec.expectStartTime.Add(task.ExecStrategy.MaxDuration).Before(now)
}

func (task *Task) ReadyExec(now time.Time) bool {
	exec := task.CurrentExecution()
	if exec == nil {
		return false
	}
	if exec.ReadyToStart(now) && exec.expectStartTime.Add(task.ExecStrategy.MaxDuration).After(now) {
		return true
	}
	return false
}

func (task *Task) ExecOvertime(now time.Time) bool {
	exec := task.CurrentExecution()
	if exec == nil {
		return false
	}

	if !exec.Executing(now) {
		return false
	}

	return exec.startTime.Add(task.ExecStrategy.MaxDuration).After(now)
}

func (task *Task) Status() Status {
	var status Status
	if task.Closed() {
		status = StatusClosed
		return status
	}

	var now = time.Now()
	if exec := task.CurrentExecution(); exec != nil {
		switch {
		case exec.WaitingStart(now) || exec.ReadyToStart(now):
			status = StatusWaitingExec
		case exec.Ended(now):
			status = StatusExecuteFinished
		case exec.Executing(now):
			status = StatusExecuting
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
	ec := task.CurrentExecution()
	if ec == nil {
		return false
	}
	now := time.Now()
	if ec.Executing(now) {
		return true
	}
	return false
}

func (task *Task) CloseExec(result ExecResult, execSeq int) bool {
	if !task.OnExecution() {
		return false
	}

	exec := task.CurrentExecution()
	if exec == nil || exec.execSeq != execSeq {
		return false
	}
	exec.result.ExecResultType = result.ExecResultType
	exec.endTime = time.Now()
	exec.result.ResultInfo = result.ResultInfo
	return true
}

func (task *Task) UpdateExecStrategy(strategy ExecStrategy) {
	task.ExecStrategy = strategy
}

func (task *Task) CancelExec(info string, execSeq int) bool {
	return task.CloseExec(ExecResult{
		info, ExecResultTypeCancel,
	}, execSeq)
}

func (task *Task) Close(closeType CloseType) {
	if task.Closed() {
		return
	}
	if task.EndTime.IsZero() {
		task.EndTime = time.Now()
	}
	task.CloseType = closeType
}

func (task *Task) Closed() bool {
	if !task.EndTime.IsZero() {
		return true
	}
	return false
}

func (task *Task) Profile() Profile {
	var ex []Execution
	for _, e := range task.Executions {
		ex = append(ex, *e)
	}
	return Profile{
		TaskKey:    task.Key,
		Status:     task.Status(),
		Executions: ex,
	}
}

type Execution struct {
	execSeq         int        `bson:"exec_seq"`
	expectStartTime time.Time  `bson:"expect_start_time"`
	createTime      time.Time  `bson:"create_time"`
	startTime       time.Time  `bson:"start_time,omitempty"`
	endTime         time.Time  `bson:"end_time,omitempty"`
	result          ExecResult `bson:"result,omitempty"`
}

func NewExecution(seq int, expectStartTime time.Time) *Execution {
	return &Execution{
		execSeq:         seq,
		expectStartTime: expectStartTime,
		createTime:      time.Now(),
	}
}

func (er *Execution) Seq() int {
	return er.execSeq
}

func (er *Execution) Start(t time.Time) {
	er.startTime = t
}

func (er *Execution) End(result ExecResult, t time.Time) {
	er.result = result
	er.endTime = t
}


func (er *Execution) ReadyToStart(t time.Time) bool {
	if er.Executing(t) || er.Ended(t) {
		return false
	}
	return er.expectStartTime.Before(t) || er.expectStartTime.Equal(t)
}

func (er *Execution) WaitingStart(t time.Time) bool {
	if er.Executing(t) || er.Ended(t) {
		return false
	}
	return er.expectStartTime.After(t)
}

func (er *Execution) Executing(t time.Time) bool {
	if er.startTime.IsZero() || er.Ended(t) {
		return false
	}
	return er.startTime.Before(t) || er.startTime.Equal(t)
}

func (er *Execution) Ended(t time.Time) bool {
	if er.endTime.IsZero() {
		return false
	}
	return er.endTime.Before(t) || er.startTime.Equal(t)
}

type ExecStrategy struct {
	ExpectStartTime time.Time
	MaxDuration     time.Duration
	MaxRetryTimes   int
	MaxRetryWait    time.Duration
	MinRetryWait    time.Duration
}

type Desc struct {
	TaskKey  string
	Strategy ExecStrategy
}

type ExecResult struct {
	ResultInfo     string
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
	TaskKey    string
	Status     Status
	Executions []Execution
}
