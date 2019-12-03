package circuit

import (
	"context"
	"errors"
	"github.com/feynman-go/workshop/randtime"
	"github.com/feynman-go/workshop/syncrun/prob"
	"golang.org/x/time/rate"
	"sync"
	"sync/atomic"
	"time"
)

var ErrLimited = errors.New("ErrLimited")

type Status int
const (
	STATUS_OPEN = 0
	STATUS_HALF_OPEN = 1
	STATUS_CLOSE = 2
)

type ExecResult struct {
	Err error
	Duration time.Duration
}

type Trigger interface {
	Reset()
	Accept(res ExecResult)
	WaitTrigger(ctx context.Context) Recovery
}

type Recovery interface {
	WaitRecovery(ctx context.Context) HalfOpener
}

type HalfOpener interface {
	Allow(ctx context.Context) bool
	Accept(res ExecResult) HalfOpenStatus
}

type HalfOpenStatus int32

const (
	HALF_OPEN_CONTINUE  HalfOpenStatus = 0
	HALF_OPEN_PASS      HalfOpenStatus = 1
	HALF_OPEN_TURN_DOWN HalfOpenStatus = 2
)

type Circuit struct {
	pb *prob.Prob
	trigger Trigger

	rw sync.RWMutex
	status Status

	halfOpener HalfOpener
	statusUpdated chan struct{}
}

func (cc *Circuit) Status() Status {
	cc.rw.RLock()
	defer cc.rw.RUnlock()
	return cc.status
}

func (cc *Circuit) StatusChange() chan struct{} {
	return cc.statusUpdated
}

// wait unit context Close
func (cc *Circuit) Do(ctx context.Context, do func(ctx context.Context) error) error {
	cc.rw.RLock()
	status := cc.status
	updateChan := cc.statusUpdated
	halfOpener := cc.halfOpener
	cc.rw.RUnlock()

	for ctx.Err() == nil {
		switch status {
		case STATUS_OPEN:
			return cc.exec(ctx, do)
		case STATUS_HALF_OPEN:
			if halfOpener.Allow(ctx) {
				return cc.exec(ctx, do)
			}
			continue
		case STATUS_CLOSE:
			select {
			case <- updateChan:
			case <- ctx.Done():
			}
			continue
		}
	}
	return ErrLimited
}

func(cc *Circuit) exec(ctx context.Context, doFunc func(ctx context.Context) error) error {
	start := time.Now()
	err := doFunc(ctx)
	cc.halfOpener.Accept(ExecResult{
		Err:      err,
		Duration: time.Now().Sub(start),
	})
	return err
}

func (cc *Circuit) runLoop(ctx context.Context) {
	for ctx.Err() == nil {
		cc.runRound(ctx)
	}
}

func (cc *Circuit) runRound(ctx context.Context) {
	trigger := cc.trigger
	trigger.Reset()
	cc.updateStatus(STATUS_HALF_OPEN)
	r := trigger.WaitTrigger(ctx)
	if r == nil {
		return
	}
	cc.updateStatus(STATUS_CLOSE)
	opener := r.WaitRecovery(ctx)
	if opener == nil {
		return
	}

	cc.rw.Lock()
	cc.halfOpener = opener
	cc.rw.Unlock()
}

func (cc *Circuit) updateStatus(status Status) {
	cc.rw.Lock()
	defer cc.rw.Unlock()
	cc.status = status
	close(cc.statusUpdated)
	cc.statusUpdated = make(chan struct{})
}

type TriggerTemplate struct {
	ErrLimiter ErrLimiter
	RecoveryFactory func(ctx context.Context) Recovery
	recoveryChan chan struct{}
	rw sync.RWMutex
}

func (trigger *TriggerTemplate) Reset() {
	if trigger.recoveryChan != nil {
		close(trigger.recoveryChan)
		trigger.recoveryChan = nil
	}
	trigger.recoveryChan = make(chan struct{})
}

func (trigger *TriggerTemplate) Accept(res ExecResult) {
	if trigger.ErrLimiter.accept(res) {
		trigger.rw.Lock()
		defer trigger.rw.Unlock()
		close(trigger.recoveryChan)
		trigger.recoveryChan = nil
	}
}

func (trigger *TriggerTemplate) WaitTrigger(ctx context.Context) Recovery {
	trigger.rw.RLock()
	cn := trigger.recoveryChan
	trigger.rw.RUnlock()
	select {
	case <- cn:
		return trigger.RecoveryFactory(ctx)
	case <- ctx.Done():
		return nil
	}
}

type ErrLimiter struct {
	ErrChecker func(err error) bool
	ErrLimiter *rate.Limiter
}

func (limiter ErrLimiter) accept(res ExecResult) (overhead bool) {
	if res.Err == nil {
		return false
	}
	if limiter.ErrLimiter == nil {
		return false
	}

	if limiter.ErrChecker != nil && !limiter.ErrChecker(res.Err){
		return false
	}

	if limiter.ErrLimiter.Allow() {
		return false
	}
	return true
}

type OneShotOpener struct {
	count int32
	ErrChecker func(error) bool
}

func (opener *OneShotOpener) Allow(ctx context.Context) bool {
	return atomic.CompareAndSwapInt32(&opener.count, 0, 1)
}
func (opener *OneShotOpener) Accept(res ExecResult) HalfOpenStatus {
	if res.Err == nil {
		return HALF_OPEN_PASS
	}
	if opener.ErrChecker != nil && !opener.ErrChecker(res.Err) {
		return HALF_OPEN_PASS
	}
	return HALF_OPEN_TURN_DOWN
}

type LatencyRecovery struct {
	MaxWaitTime time.Duration
	MiniWaitTime time.Duration
	DoRecover func(ctx context.Context) error
	OpenerFactory func() HalfOpener
}

func (recovery LatencyRecovery) WaitRecovery(ctx context.Context) HalfOpener {
	dur := randtime.RandDuration(recovery.MiniWaitTime, recovery.MaxWaitTime)
	timer := time.NewTimer(dur)
	select {
	case <- ctx.Done():
		return nil
	case <- timer.C:
		return recovery.OpenerFactory()
	}
}





