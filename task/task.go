package task

import (
	"errors"
	"time"
)

var ErrOverMaxRetry = errors.New("over max retry")

var ErrExecuting = errors.New("is executing")

const (
	statusUnavailable     statusCode = 1
	statusWaitingExec     statusCode = 2
	statusExecuting       statusCode = 3
	statusExecuteFinished statusCode = 4
)

type statusCode int

func (status statusCode) String() string {
	switch status {
	case statusUnavailable:
		return "unavailable"
	case statusWaitingExec:
		return "waiting exec"
	case statusExecuting:
		return "executing"
	case statusExecuteFinished:
		return "exec finished"
	default:
		return "unknown"
	}
}

type Desc struct {
	TaskKey  string
	ExecDesc ExecConfig
	Tags     []string
	Overlap  bool
}

type Schedule struct {
	AwakenTime time.Time
	Priority   int32
}

type Task struct {
	Key string        `bson:"_id"`
	Seq  int64         `bson:"seq"`
	Schedule Schedule `bson:"schedule"`
	Info Info `bson:"info"`
	Execution Execution `bson:"execution"`
}

func (t *Task) NewExec() {
	t.Execution = Execution {
		Available: true,
		Config: t.Info.ExecConfig,
		CreateTime: time.Now(),
	}
}

/*func (t *Task) Seq(now time.Time) int64 {
	er := t.Execution
	switch  {
	case er.Available:
		return (int64(t.MainSeq) << 32) | int64(statusUnavailable)
	case er.WaitingStart(now):
		return (int64(t.MainSeq) << 32) | int64(statusWaitingExec)
	case er.Executing(now):
		return (int64(t.MainSeq) << 32) | int64(statusExecuting)
	case er.Ended(now):
		return (int64(t.MainSeq) << 32) | int64(statusExecuteFinished)
	default:
		return int64(t.MainSeq) << 32
	}
}*/

/*func (task Task) Copy() Task {
	ret := task
	if task.Execution != nil {
		ret.Execution = new(Execution)
		*ret.Execution = *task.Execution
	}
	return ret
}*/

type Info struct {
	Tags       []string   `bson:"tags"`
	ExecConfig ExecConfig `bson:"execDesc"`
	CreateTime time.Time `bson:"createTime"`
	ExecCount int32 `bson:"execCount"`
}

type Execution struct {
	Available       bool
	Config          ExecConfig    `bson:"config"`
	CreateTime      time.Time     `bson:"createTime,omitempty"`
	StartTime       time.Time     `bson:"startTime,omitempty"`
	EndTime         time.Time     `bson:"endTime,omitempty"`
	Result          ExecResult    `bson:"result,omitempty"`
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



type ExecResult struct {
	ResultInfo string     `bson:"resultInfo"`
	ResultCode int64      `bson:"resultCode"`
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
	StatusCount map[statusCode]int64
}

type StatusProfile struct {
	Total    int64     `json:"total"`
	Count    int64     `json:"count"`
	Profiles []Profile `json:"profile"`
}

type Profile struct {
	TaskKey    string
	Status     statusCode
	Executions []Execution
}