package task

import (
	"errors"
	"time"
)

var ErrOverMaxRetry = errors.New("over max retry")

var ErrExecuting = errors.New("is executing")

const (
	StatusUnknown         StatusCode = 0
	StatusUnavailable     StatusCode = 1
	StatusWaitingExec     StatusCode = 2
	StatusExecuting       StatusCode = 3
	StatusExecuteFinished StatusCode = 4
)

type StatusCode int

func (status StatusCode) String() string {
	switch status {
	case StatusUnavailable:
		return "unavailable"
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

type Schedule struct {
	AwakenTime         time.Time `bson:"awaken"`
	CompensateDuration time.Duration `bson:"compensate"`
}

type Task struct {
	Key       string    `bson:"_id"`
	Stage     int64     `bson:"stage"`
	Schedule  Schedule  `bson:"schedule"` // schedule info for scheduler to schedule
	Meta      Meta      `bson:"meta"`      // task meta info
	Execution Execution `bson:"execution"` // current execution info
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
		return StatusWaitingExec
	case er.Executing():
		return StatusExecuting
	case er.Ended():
		return StatusExecuteFinished
	case er.Available:
		return StatusUnavailable
	default:
		return StatusUnknown
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

type Meta struct {
	Tags           []string   `bson:"tags"`
	InitExecOption ExecOption `bson:"execDesc"`
	CreateTime     time.Time  `bson:"createTime"`
	StartCount     int32      `bson:"restartCount"`
	TotalExecCount int32      `bson:"execCount"`
	Exclusive      bool       `bson:"exclusive"` //是否独占的, 排他的
}

func (info Meta) canRestart() bool {
	if info.InitExecOption.MaxRecover == nil {
		return false
	}
	return info.StartCount < *info.InitExecOption.MaxRecover
}

type Execution struct {
	Available    bool       `bson:"available"`
	Config       ExecOption `bson:"config"`
	CreateTime   time.Time  `bson:"createTime,omitempty"`
	StartTime    time.Time  `bson:"startTime,omitempty"`
	EndTime      time.Time  `bson:"endTime,omitempty"`
	Result       Result     `bson:"result,omitempty"`
	LastKeepLive time.Time  `bson:"lastKeepLive,omitempty"`
}

func (er *Execution) Start(t time.Time) {
	er.StartTime = t
}

func (er *Execution) End(result Result, t time.Time) {
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

type Result struct {
	ResultInfo string     `bson:"resultInfo"`
	ResultCode int64      `bson:"resultCode"`
	Continue   bool       `bson:"continue"`
	NextExec   ExecOption `bson:"next,omitempty"`
}

func (er *Result) SetResult(info string, customerCode int64) {
	er.ResultInfo = info
	er.ResultCode = customerCode
}

func (er *Result) SetFinish() {
	er.Continue = false
	return
}

func (er *Result) SetWaitAndReDo(wait time.Duration) *Result {
	er.Continue = true
	er.NextExec = er.NextExec.SetExpectStartTime(time.Now().Add(wait))
	return er
}

// keep current task alive and return
func (er *Result) SetReturnWithLive(keepLive time.Duration) *Result {
	er.Continue = true
	er.NextExec = er.NextExec.SetCompensateDuration(keepLive)
	return er
}

func (er *Result) SetMaxDuration(maxDuration time.Duration) *Result {
	er.NextExec = er.NextExec.SetMaxExecDuration(maxDuration)
	return er
}

func (er *Result) SetMaxRecover(maxRecover int32) *Result {
	er.NextExec = er.NextExec.SetMaxRecoverCount(maxRecover)
	return er
}

type Summery struct {
	StatusCount map[StatusCode]int64
}

