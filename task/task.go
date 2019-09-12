package task

import (
	"errors"
	"time"
)

var ErrOverMaxRetry = errors.New("over max retry")

var ErrExecuting = errors.New("is executing")

const (
	statusUnknown     StatusCode = 0
	statusUnavailable     StatusCode = 1
	statusWaitingExec     StatusCode = 2
	statusExecuting       StatusCode = 3
	statusExecuteFinished StatusCode = 4
)

type StatusCode int

func (status StatusCode) String() string {
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

//type Desc struct {
//	TaskKey  string
//	ExecDesc ExecOption
//	Tags     []string
//	Overlap  bool
//}

type Schedule struct {
	AwakenTime         time.Time
	CompensateDuration time.Duration
}

type Task struct {
	Key       string    `bson:"_id"`
	Stage     int64     `bson:"stage"`
	Schedule  Schedule  `bson:"schedule"`
	Info      Info      `bson:"info"`
	Execution Execution `bson:"execution"`
}

func (t *Task) NewExec(stageID int64, option ExecOption) {
	t.Stage = stageID
	t.Execution = Execution {
		Available: true,
		Config: option,
		CreateTime: time.Now(),
	}
}

func (t *Task) Status() StatusCode {
	er := t.Execution
	switch  {
	case er.WaitingStart():
		return statusWaitingExec
	case er.Executing():
		return statusExecuting
	case er.Ended():
		return statusExecuteFinished
	case er.Available:
		return statusUnavailable
	default:
		return statusUnknown
	}
}

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
	ExecOption ExecOption `bson:"execDesc"`
	CreateTime time.Time  `bson:"createTime"`
	StartCount int32      `bson:"restartCount"`
	ExecCount  int32      `bson:"execCount"`
}

func (info Info) CanRestart() bool {
	if info.ExecOption.MaxRestart == nil {
		return false
	}
	return info.StartCount < *info.ExecOption.MaxRestart
}

type Execution struct {
	Available    bool       `bson:"available"`
	Config       ExecOption `bson:"config"`
	CreateTime   time.Time  `bson:"createTime,omitempty"`
	StartTime    time.Time  `bson:"startTime,omitempty"`
	EndTime      time.Time  `bson:"endTime,omitempty"`
	Result       ExecResult `bson:"result,omitempty"`
	LastKeepLive time.Time  `bson:"lastKeepLive,omitempty"`
}

func (er *Execution) Start(t time.Time) {
	er.StartTime = t
}

func (er *Execution) End(result ExecResult, t time.Time) {
	er.Result = result
	er.EndTime = t
}

func (er *Execution) ReadyToStart() bool {
	if er.Executing() || er.Ended() {
		return false
	}
	t := time.Now()
	expect := er.Config.GetExpectTime()
	return expect.Before(t) || expect.Equal(t)
}

func (er *Execution) WaitingStart() bool {
	if er.Executing() || er.Ended() {
		return false
	}
	return er.Config.GetExpectTime().After(time.Now())
}

func (er *Execution) Executing() bool {
	if er.StartTime.IsZero() || er.Ended() {
		return false
	}
	return true
}

func (er *Execution) Ended() bool {
	return !er.EndTime.IsZero()
}

func (er *Execution) IsDead(t time.Time) bool {
	if !er.Executing() {
		return false
	}
	// check keep live
	var compensateDuration time.Duration
	if er.Config.CompensateDuration != nil {
		compensateDuration = *er.Config.CompensateDuration
	}

	if compensateDuration > 0 {
		if er.LastKeepLive.IsZero() { // no keep live tim
			return er.StartTime.Add(compensateDuration * 2).Before(t)
		} else {
			return er.LastKeepLive.Add(compensateDuration * 2).Before(t)
		}
	}
	return false
}

func (er *Execution) OverExecTime(t time.Time) bool {
	if !er.Executing() {
		return false
	}
	var maxExecDuration time.Duration
	if er.Config.MaxExecDuration != nil {
		maxExecDuration = *er.Config.MaxExecDuration
	}

	startTime := er.Config.GetExpectTime()
	if startTime.IsZero() {
		startTime = er.StartTime
	}

	lastTime := startTime.Add(maxExecDuration)
	return lastTime.Before(t)
}

type ExecResult struct {
	ResultInfo string     `bson:"resultInfo"`
	ResultCode int64      `bson:"resultCode"`
	Continue   bool       `bson:"continue"`
	NextExec   ExecOption `bson:"next,omitempty"`
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