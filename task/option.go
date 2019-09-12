package task

import (
	"runtime"
	"time"
)

func DefaultManagerOption () ManagerOption{
	return ManagerOption {
		MaxBusterTask: runtime.NumGoroutine() * 8,
		DefaultExecMaxDuration: time.Minute,
		DefaultRetryCount: 3,
		DefaultCompensateDuration: time.Minute,
		WaitCloseDuration: 10 * time.Second,
	}
}

type ManagerOption struct {
	MaxBusterTask          int
	DefaultExecMaxDuration time.Duration
	DefaultRetryCount 		   int32
	DefaultCompensateDuration     time.Duration
	WaitCloseDuration      time.Duration
}

func (mOpt ManagerOption) Option() Option {
	opt := Option{}
	if mOpt.DefaultCompensateDuration != 0 {
		opt = opt.SetKeepLiveDuration(mOpt.DefaultCompensateDuration)
	}
	if mOpt.DefaultExecMaxDuration != 0 {
		opt = opt.SetMaxExecDuration(mOpt.DefaultExecMaxDuration)
	}
	if mOpt.DefaultRetryCount != 0 {
		opt = opt.SetMaxRestartCount(mOpt.DefaultRetryCount)
	}

	return opt
}

func (mOpt ManagerOption) CompleteWith(dmp ManagerOption) ManagerOption {
	if mOpt.WaitCloseDuration == 0 {
		mOpt.WaitCloseDuration = dmp.WaitCloseDuration
	}
	if mOpt.MaxBusterTask == 0 {
		mOpt.MaxBusterTask = dmp.MaxBusterTask
	}
	if mOpt.DefaultExecMaxDuration == 0 {
		mOpt.DefaultExecMaxDuration = dmp.DefaultExecMaxDuration
	}
	if mOpt.DefaultRetryCount == 0 {
		mOpt.DefaultRetryCount = dmp.DefaultRetryCount
	}
	if mOpt.DefaultCompensateDuration == 0 {
		mOpt.DefaultCompensateDuration = dmp.DefaultCompensateDuration
	}
	return mOpt
}

type Option struct {
	Exec ExecOption
	Overlap *bool
	Tags *[]string
}

func (opt Option) SetTags(tags []string) Option {
	opt.Tags = &tags
	return opt
}

func (opt Option) SetOverLap(over bool) Option {
	opt.Overlap = &over
	return opt
}

func (opt Option) SetExpectStartTime(expect time.Time) Option {
	opt.Exec = opt.Exec.SetExpectStartTime(expect)
	return opt
}

func (opt Option) SetMaxExecDuration(maxExec time.Duration) Option {
	opt.Exec = opt.Exec.SetMaxExecDuration(maxExec)
	return opt
}

func (opt Option) SetMaxRestartCount(count int32) Option {
	opt.Exec = opt.Exec.SetMaxRetryCount(count)
	return opt
}

func (opt Option) SetKeepLiveDuration(duration time.Duration) Option {
	opt.Exec = opt.Exec.SetCompensateDuration(duration)
	return opt
}

func (opt Option) Merge(option Option) Option {
	newOpt := opt
	newOpt.Exec = newOpt.Exec.Merge(option.Exec)
	if option.Tags != nil {
		newOpt.Tags = option.Tags
	}
	if option.Overlap != nil {
		newOpt.Overlap = option.Overlap
	}
	return newOpt
}

type ExecOption struct {
	ExpectStartTime    *time.Time     `bson:"expectRetryTime,omitempty"`
	MaxExecDuration    *time.Duration `bson:"maxExecDuration,omitempty"`
	MaxRestart         *int32         `bson:"remainExec,omitempty"`
	CompensateDuration *time.Duration `bson:"compensateDuration,omitempty"`
}

func (e ExecOption) Merge(option ExecOption) ExecOption {
	newOpt := e
	if option.CompensateDuration != nil {
		newOpt.CompensateDuration = option.CompensateDuration
	}
	if option.MaxRestart != nil {
		newOpt.MaxRestart = option.MaxRestart
	}
	if option.MaxExecDuration != nil {
		newOpt.MaxExecDuration = option.MaxExecDuration
	}
	if option.ExpectStartTime != nil {
		newOpt.ExpectStartTime = option.ExpectStartTime
	}
	return newOpt
}

func (e ExecOption) SetExpectStartTime(expect time.Time) ExecOption {
	e.ExpectStartTime = &expect
	return e
}

func (e ExecOption) SetMaxExecDuration(dur time.Duration) ExecOption {
	e.MaxExecDuration = &dur
	return e
}

func (e ExecOption) SetMaxRetryCount(count int32) ExecOption {
	e.MaxRestart = &count
	return e
}

func (e ExecOption) SetCompensateDuration(dur time.Duration) ExecOption {
	e.CompensateDuration = &dur
	return e
}

func (e ExecOption) SetFinished() ExecOption {
	return e.SetMaxRetryCount(0)
}

func (e ExecOption) GetExpectTime() time.Time {
	var expect = time.Time{}
	if e.ExpectStartTime != nil  {
		expect = *e.ExpectStartTime
	}
	return expect
}


func (e ExecOption) AddRemainExecCount(delta int32) ExecOption {
	var ct int32
	if e.MaxRestart != nil {
		ct = *e.MaxRestart
	}
	ct += delta
	if ct < 0 {
		ct = 0
	}
	return e.SetMaxRetryCount(ct)
}


func (e ExecOption) GetExpectStartSchedule() Schedule {
	var (
		expectStart = time.Now()
		compensateDuration = time.Duration(0)
	)

	if e.ExpectStartTime != nil && e.ExpectStartTime.After(expectStart) {
		expectStart = *e.ExpectStartTime
	}

	if e.CompensateDuration != nil {
		compensateDuration = *e.CompensateDuration
	}

	return Schedule{
		AwakenTime: expectStart,
		CompensateDuration: compensateDuration,
	}
}


func (e ExecOption) GetExecutingSchedule() Schedule {
	var (
		awakeTime = time.Now()
		compensateDuration time.Duration
	)
	if e.CompensateDuration != nil {
		compensateDuration = *e.CompensateDuration
	}

	if e.MaxExecDuration != nil {
		if *e.MaxExecDuration < compensateDuration {
			compensateDuration = *e.MaxExecDuration
		}
	}

	// TODO  need check real start time / expect start time
	return Schedule{
		AwakenTime: awakeTime.Add(compensateDuration),
		CompensateDuration: compensateDuration,
	}
}