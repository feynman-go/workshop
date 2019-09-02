package task

import (
	"errors"
	"time"
)

var ErrOverMaxRetry = errors.New("over max retry")

var ErrExecuting = errors.New("is executing")

const (
	StatusCreated         StatusCode = 0
	StatusInit            StatusCode = 1 << 10
	StatusWaitingExec     StatusCode = 2 << 10
	StatusExecuting       StatusCode = 3 << 10
	StatusExecuteFinished StatusCode = 4 << 10
	StatusClosed          StatusCode = 5 << 10
	StatusUnknown         StatusCode = 128 << 10
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

type Schedule struct {
	AcceptTime time.Time
	ExpectTime time.Time
	Priority int32
}

type Task struct {
	Key             string        `bson:"_id"`
	Schedule Schedule `bson:"inner"`
	Info Info `bson:"info"`
}

type Info struct {

	ExecutionID int64 `bson:"execID,omitempty"`
	Tags        []string `bson:"tags"`
	Executing   bool `bson:"execution"`
	MaxExecCount int32 `bson:"maxExec"`
	ExecCount 	int32 `bson:"execCount,omitempty"`
	MaxExecDuration time.Duration `bson:"maxExecDur"`
}

type Execution struct {
	ID int64 `bson:"expect_start_time"`
	TaskID string `bson:"taskID"`
	ExpectStartTime time.Time  `bson:"expect_start_time"`
	CreateTime      time.Time  `bson:"create_time"`
	StartTime       time.Time  `bson:"start_time,omitempty"`
	EndTime         time.Time  `bson:"end_time,omitempty"`
	MaxExecDuration time.Duration  `bson:"maxExecDur,omitempty"`
	Result          ExecResult `bson:"result,omitempty"`
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
	return er.ExpectStartTime.Before(t) || er.ExpectStartTime.Equal(t)
}

func (er *Execution) WaitingStart(t time.Time) bool {
	if er.Executing(t) || er.Ended(t) {
		return false
	}
	return er.ExpectStartTime.After(t)
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
	return er.Result.RetryInfo != nil
}

func (er *Execution) OverExecTime(t time.Time) bool {
	if !er.Executing(t) {
		return false
	}
	lastTime := er.ExpectStartTime.Add(er.MaxExecDuration)
	return lastTime.Before(t)
}


type Desc struct {
	TaskKey         string
	ExpectStartTime time.Time
	MaxExecCount    int32
	MaxExecDuration time.Duration
	Tags            []string
	Priority        int32
}

type ExecResult struct {
	ResultInfo  string  `bson:"resultInfo"`
	RetryInfo *ExecDesc `bson:"omitempty"`
}

type ExecDesc struct {
	ExpectRetryTime time.Time `bson:"expectRetryTime"`
	MaxExecDuration time.Duration `bson:"maxExecDuration"`
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