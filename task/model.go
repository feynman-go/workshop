package task

import (
	"errors"
	"time"
)

var ErrOverMaxRetry = errors.New("over max retry")

var ErrExecuting = errors.New("is executing")

const (
	StatusWaitingExec     StatusCode = 2
	StatusExecuting       StatusCode = 3
	StatusExecuteFinished StatusCode = 4
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

type StatusCode int

func (status StatusCode) String() string {
	switch status {
	case StatusWaitingExec:
		return "waiting exec"
	case StatusExecuting:
		return "executing"
	case StatusExecuteFinished:
		return "exec finished"
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

type Schedule struct {
	AwakenTime time.Time
	Priority   int32
}

type Task struct {
	Key string        `bson:"_id"`
	Schedule Schedule `bson:"schedule"`
	Info Info `bson:"info"`
	Execution *Execution `bson:"execution"`
}

func (task *Task) NewExec(execSeq int32) {
	task.Execution = &Execution{
		MainSeq: execSeq,
		Config: task.Info.ExecConfig,
	}
}

func (task Task) Copy() Task {
	ret := task
	if task.Execution != nil {
		ret.Execution = new(Execution)
		*ret.Execution = *task.Execution
	}
	return ret
}



type Info struct {
	Tags       []string   `bson:"tags"`
	ExecConfig ExecConfig `bson:"execDesc"`
	CreateTime time.Time `bson:"createTime"`
}

type Execution struct {
	MainSeq         int32         `bson:"mainSeq"`
	Config          ExecConfig    `bson:"config"`
	CreateTime      time.Time     `bson:"createTime,omitempty"`
	StartTime       time.Time     `bson:"startTime,omitempty"`
	EndTime         time.Time     `bson:"endTime,omitempty"`
	Result          ExecResult    `bson:"result,omitempty"`
}

func (er *Execution) Seq(t time.Time) int64 {
	switch  {
	case er.WaitingStart(t):
		return (int64(er.MainSeq) << 32) | int64(StatusWaitingExec)
	case er.Executing(t):
		return (int64(er.MainSeq) << 32) | int64(StatusExecuting)
	case er.Ended(t):
		return (int64(er.MainSeq) << 32) | int64(StatusExecuteFinished)
	default:
		return int64(er.MainSeq) << 32
	}
}


func (er *Execution) Start(t time.Time) {
	er.StartTime = t
}

func (er *Execution) End(result ExecResult, t time.Time) {
	er.Result = result
	er.EndTime = t
}

func (er *Execution) ReadyToStart(t time.Time) bool {
	if er.Executing(t) || er.Ended(t) {
		return false
	}
	return er.Config.ExpectStartTime.Before(t) || er.Config.ExpectStartTime.Equal(t)
}

func (er *Execution) WaitingStart(t time.Time) bool {
	if er.Executing(t) || er.Ended(t) {
		return false
	}
	return er.Config.ExpectStartTime.After(t)
}

func (er *Execution) Executing(t time.Time) bool {
	if er.StartTime.IsZero() || er.Ended(t) {
		return false
	}
	return er.StartTime.Before(t) || er.StartTime.Equal(t)
}

func (er *Execution) Ended(t time.Time) bool {
	if er.EndTime.IsZero() {
		return false
	}
	return er.EndTime.Before(t) || er.StartTime.Equal(t)
}

func (er *Execution) CanRetry() bool {
	return er.Result.NextExec.RemainExecCount > 0
}

func (er *Execution) OverExecTime(t time.Time) bool {
	if !er.Executing(t) {
		return false
	}
	lastTime := er.Config.ExpectStartTime.Add(er.Config.MaxExecDuration)
	return lastTime.Before(t)
}


type Desc struct {
	TaskKey  string
	ExecDesc ExecConfig
	Tags     []string
}

type ExecResult struct {
	ResultInfo string     `bson:"resultInfo"`
	NextExec   ExecConfig `bson:"next,omitempty"`
}

type ExecConfig struct {
	ExpectStartTime time.Time     `bson:"expectRetryTime"`
	MaxExecDuration time.Duration `bson:"maxExecDuration"`
	RemainExecCount int32         `bson:"remainExec"`
	Priority        int32         `bson:"priority"`
}

type ExecSummery struct {
	CurrentIndex int32
	ExpectExecTime time.Time
	MaxDuration time.Duration
}

type Summery struct {
	StatusCount map[StatusCode]int64
}

type StatusProfile struct {
	Total    int64     `json:"total"`
	Count    int64     `json:"count"`
	Profiles []Profile `json:"profile"`
}

type Profile struct {
	TaskKey    string
	Status     StatusCode
	Executions []Execution
}