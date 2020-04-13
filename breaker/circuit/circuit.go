package circuit

import (
	"context"
	"errors"
	"github.com/feynman-go/workshop/syncrun/routine"
	"golang.org/x/time/rate"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

var ErrLimited = errors.New("ErrLimited")

type Status int
const (
	STATUS_OPEN             = 0
	STATUS_WAITING_RECOVERY = 1
)

type ExecResult struct {
	Err error
	Duration time.Duration
}

type Trigger interface {
	Reset()
	AcceptResult(res ExecResult)
	WaitTrigger(ctx context.Context) Recovery
}

type Recovery interface {
	// return nil if not allow
	WaitAllow(ctx context.Context) RecoveryAcceptor
	WaitRecovery(ctx context.Context)
}

type RecoveryAcceptor func(res ExecResult)

type HalfOpenStatus int32

const (
	HALF_OPEN_CONTINUE  HalfOpenStatus = 0
	HALF_OPEN_PASS      HalfOpenStatus = 1
	HALF_OPEN_TURN_DOWN HalfOpenStatus = 2
)

type Circuit struct {
	pb *routine.Routine
	trigger Trigger

	rw sync.RWMutex
	status Status

	recovery Recovery

	statusUpdated chan struct{}
}

func New(trigger Trigger) *Circuit {
	c := &Circuit{
		pb:            nil,
		trigger:       trigger,
		status:        STATUS_OPEN,
		statusUpdated: make(chan struct{}),
	}
	c.pb = routine.New(c.runLoop)
	c.pb.Start()
	<- c.pb.Running()
	return c
}

func (cc *Circuit) Status() Status {
	cc.rw.RLock()
	defer cc.rw.RUnlock()
	return cc.status
}

func (cc *Circuit) StatusChange() chan struct{} {
	cc.rw.RLock()
	defer cc.rw.RUnlock()
	return cc.statusUpdated
}

// wait unit context Close
func (cc *Circuit) Do(ctx context.Context, do func(ctx context.Context) error) error {
	cc.rw.RLock()
	status := cc.status
	updateChan := cc.statusUpdated
	cc.rw.RUnlock()

	for ctx.Err() == nil {
		switch status {
		case STATUS_OPEN:
			return cc.execOnOpen(ctx, do)
		case STATUS_WAITING_RECOVERY:
			err := cc.execOnRecovering(ctx, do)
			if err == nil {
				return nil
			}
			if err != ErrLimited {
				return err
			}
			select {
			case <- updateChan:
			case <- ctx.Done():
			}
			continue
		}
	}
	return ErrLimited
}

func(cc *Circuit) execOnRecovering(ctx context.Context, doFunc func(ctx context.Context) error) error {
	var (
		err error
		recovery = cc.recovery
	)
	if recovery == nil {
		return ErrLimited
	}

	if acceptor := recovery.WaitAllow(ctx); acceptor != nil {
		start := time.Now()
		err = doFunc(ctx)
		acceptor(ExecResult{
			Err:      err,
			Duration: time.Now().Sub(start),
		})
	} else {
		return ErrLimited
	}
	return err
}

func(cc *Circuit) execOnOpen(ctx context.Context, doFunc func(ctx context.Context) error) error {
	start := time.Now()
	err := doFunc(ctx)
	if trigger := cc.trigger; trigger != nil {
		trigger.AcceptResult(ExecResult{
			Err:      err,
			Duration: time.Now().Sub(start),
		})
	}
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

	cc.rw.Lock()
	cc.updateStatus(STATUS_OPEN)
	cc.rw.Unlock()

	r := trigger.WaitTrigger(ctx)
	if r == nil {
		return
	}

	cc.rw.Lock()
	cc.updateStatus(STATUS_WAITING_RECOVERY)
	cc.recovery = r
	cc.rw.Unlock()

	// break if recovered
	r.WaitRecovery(ctx)

	cc.rw.Lock()
	cc.updateStatus(STATUS_OPEN)
	cc.recovery = nil
	cc.rw.Unlock()
}

func (cc *Circuit) updateStatus(status Status) {
	cc.status = status
	close(cc.statusUpdated)
	cc.statusUpdated = make(chan struct{})
}

type TriggerTemplate struct {
	errLimiter      ErrLimiter
	recoveryFactory func(ctx context.Context) Recovery
	recoveryChan    chan struct{}
	rw              sync.RWMutex
}

func NewSimpleTrigger(errLimiter ErrLimiter, recoveryFactory func(ctx context.Context) Recovery) Trigger {
	return &TriggerTemplate {
		errLimiter: errLimiter,
		recoveryChan: make(chan struct{}),
		recoveryFactory: recoveryFactory,
	}
}

func (trigger *TriggerTemplate) Reset() {
	if trigger.recoveryChan != nil {
		close(trigger.recoveryChan)
		trigger.recoveryChan = nil
	}
	trigger.recoveryChan = make(chan struct{})
}

func (trigger *TriggerTemplate) AcceptResult(res ExecResult) {
	if trigger.errLimiter.accept(res) {
		trigger.rw.Lock()
		defer trigger.rw.Unlock()
		if trigger.recoveryChan != nil {
			close(trigger.recoveryChan)
		}
		trigger.recoveryChan = nil
	}
}

func (trigger *TriggerTemplate) WaitTrigger(ctx context.Context) Recovery {
	trigger.rw.Lock()
	cn := trigger.recoveryChan
	if cn == nil {
		cn = make(chan struct{})
	}
	trigger.recoveryChan = cn
	trigger.rw.Unlock()
	select {
	case <- cn:
		if trigger.recoveryFactory != nil {
			return trigger.recoveryFactory(ctx)
		}
		return nil
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


type HalfOpenRecovery struct {
	ResetFunc func(ctx context.Context) *HalfOpener
	readyChan chan struct{}
	halfOpener *HalfOpener
	rw sync.RWMutex
}

func (recovery *HalfOpenRecovery) WaitRecovery(ctx context.Context) {
	reSetter := recovery.ResetFunc
	if reSetter == nil {
		reSetter = func(ctx context.Context) *HalfOpener {
			// default half opener
			return NewHalfOpener(rate.NewLimiter(0,1), nil, 1)
		}
	}

	for ctx.Err() == nil {
		// create ready chan
		recovery.rw.Lock()
		recovery.readyChan = make(chan struct{})
		recovery.rw.Unlock()

		halfOpener := recovery.ResetFunc(ctx)

		recovery.rw.Lock()
		recovery.halfOpener = halfOpener
		close(recovery.readyChan)
		recovery.rw.Unlock()

		if halfOpener == nil {
			return
		}

		if halfOpener.WaitRecovered(ctx) {
			halfOpener.Close()
			return
		}
		halfOpener.Close()
	}
	return
}

func (recovery *HalfOpenRecovery) WaitAllow(ctx context.Context) RecoveryAcceptor {
	var (
		readyChan chan struct{}
		halfOpener *HalfOpener
	)

	for  {
		if ctx.Err() != nil {
			return nil
		}
		recovery.rw.RLock()
		readyChan = recovery.readyChan
		recovery.rw.RUnlock()

		if readyChan != nil {
			break
		}

		runtime.Gosched()
	}

	select {
	case <- ctx.Done():
		return nil
	case <- recovery.readyChan:
	}

	recovery.rw.RLock()
	halfOpener = recovery.halfOpener
	recovery.rw.RUnlock()

	if halfOpener == nil {
		return nil
	}

	if !halfOpener.WaitAllow(ctx) {
		return nil
	}

	return halfOpener.Accept
}

func NewHalfOpener(doLimiter *rate.Limiter, errChecker func(error) bool, maxCount int32) *HalfOpener {
	if doLimiter == nil {
		doLimiter = rate.NewLimiter(0, 1)
	}
	if maxCount <= 0 {
		maxCount = 1
	}
	return &HalfOpener{
		DoLimiter:  doLimiter,
		ErrChecker: nil,
		MaxCount:   maxCount,
		count:      0,
		cn:         make(chan struct{}, 1),
		closeCn:    make(chan struct{}),
		rw:         sync.RWMutex{},
		status:     HALF_OPEN_CONTINUE,
	}
}

type HalfOpener struct {
	DoLimiter *rate.Limiter
	ErrChecker func(error) bool
	MaxCount int32
	count int32
	cn chan struct{}
	closeCn chan struct{}
	rw sync.RWMutex

	status HalfOpenStatus
}

func (opener *HalfOpener) WaitRecovered(ctx context.Context) bool {
	cn := opener.cn
	closeCn := opener.closeCn
	for {
		if opener.DoLimiter.Wait(ctx) != nil {
			return false
		}
		select {
		case <- ctx.Done():
			return false
		case cn <- struct{}{}:
			continue
		case <- closeCn:
			opener.rw.RLock()
			var status = opener.status
			opener.rw.RUnlock()
			return status == HALF_OPEN_PASS
		}
	}
}

func (opener *HalfOpener) WaitAllow(ctx context.Context) bool {
	opener.rw.RLock()
	var status = opener.status
	opener.rw.RUnlock()
	if status == HALF_OPEN_PASS {
		return true
	}
	if status == HALF_OPEN_TURN_DOWN {
		return false
	}

	select {
	case <- ctx.Done():
		return false
	case <- opener.cn:
		return true
	case <- opener.closeCn:
		opener.rw.RLock()
		status = opener.status
		opener.rw.RUnlock()
		if status == HALF_OPEN_PASS {
			return true
		}
		return false
	}
}

func (opener *HalfOpener) Accept(res ExecResult) {
	if res.Err == nil {
		if atomic.AddInt32(&opener.count, 1) >= opener.MaxCount {
			opener.rw.Lock()
			if opener.status == HALF_OPEN_CONTINUE {
				opener.status = HALF_OPEN_PASS
				close(opener.closeCn)
			}
			opener.rw.Unlock()
		}
		return
	}

	if opener.ErrChecker != nil && !opener.ErrChecker(res.Err) {
		return
	}

	opener.rw.Lock()
	if opener.status == HALF_OPEN_CONTINUE {
		opener.status = HALF_OPEN_TURN_DOWN
		close(opener.closeCn)
	}
	opener.rw.Unlock()
	return
}

func (opener *HalfOpener) Close() {
	opener.rw.Lock()
	defer opener.rw.Unlock()
	if opener.status == HALF_OPEN_CONTINUE {
		opener.status = HALF_OPEN_TURN_DOWN
		close(opener.closeCn)
	}
}
