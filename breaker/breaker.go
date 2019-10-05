package breaker

import (
	"context"
	"github.com/feynman-go/workshop/syncrun/prob"
	"golang.org/x/time/rate"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type Breaker struct {
	rw       sync.RWMutex
	watcher  RecoverableWatcher
	recovery WatchableRecovery
	pb       *prob.Prob
	term     int
	onChan chan struct{}
	offChan chan struct{}
}

func New(watcher RecoverableWatcher) *Breaker {
	on := make(chan struct{})
	off := make(chan struct{})
	close(off)
	breaker := &Breaker {
		watcher: watcher,
		onChan: on,
		offChan: off,
	}
	breaker.pb = prob.New(breaker.run)
	breaker.pb.Start()
	return breaker
}

func (breaker *Breaker) Accept(abnormal bool) {
	breaker.rw.RLock()
	acceptor := breaker.watcher
	breaker.rw.RUnlock()

	if acceptor != nil {
		acceptor.Accept(abnormal)
	}
}

func (breaker *Breaker) OnChan() <- chan struct{} {
	return breaker.onChan
}

func (breaker *Breaker) OffChan() <- chan struct{} {
	return breaker.offChan
}


func (breaker *Breaker) Reset(acceptor RecoverableWatcher) {
	if acceptor == nil {
		panic("accept is nil")
	}
	breaker.rw.Lock()
	breaker.pb.Stop()
	breaker.pb = prob.New(breaker.run)
	breaker.recovery = nil
	breaker.watcher = acceptor
	select {
	case <- breaker.offChan:
	case <- breaker.onChan:
		close(breaker.onChan)
		breaker.offChan = make(chan struct{})
	default:
	}
	breaker.rw.Unlock()
}

func (breaker *Breaker) run(ctx context.Context) {
	for ctx.Err() == nil {
		breaker.rw.RLock()
		acceptor := breaker.watcher
		recovery := breaker.recovery
		breaker.rw.RUnlock()

		if acceptor != nil {
			r := acceptor.WaitUnacceptable(ctx)
			breaker.rw.Lock()
			breaker.watcher = nil
			breaker.recovery = r
			select {
			case <- breaker.onChan:
			default:
				close(breaker.onChan)
			}
			breaker.onChan = make(chan struct{})
			breaker.rw.Unlock()
		} else if recovery != nil {
			a := recovery.WaitRecovery(ctx)
			breaker.rw.Lock()
			breaker.term ++
			breaker.recovery = nil
			breaker.watcher = a
			select{
			case <- breaker.offChan:
			default:
				close(breaker.offChan)
			}
			breaker.offChan = make(chan struct{})
			breaker.rw.Unlock()
		} else {
			<- ctx.Done()
		}
	}
}

func (breaker *Breaker) Opening() bool {
	breaker.rw.RLock()
	off := breaker.offChan
	on := breaker.offChan
	breaker.rw.RUnlock()
	select {
	case <- off:
		return false
	case <- on:
		return true
	default:
		return false
	}
}


type Recovery interface {
	WaitRecovery(ctx context.Context)
}

type Watcher interface {
	// accept record
	Accept(abnormal bool)

	// wait to recovery status. (means break open)
	WaitUnacceptable(ctx context.Context)
}

type RecoverableWatcher interface {
	Accept(abnormal bool)
	WaitUnacceptable(ctx context.Context) WatchableRecovery
}

type WatchableRecovery interface {
	WaitRecovery(ctx context.Context) RecoverableWatcher
}

// 三项熔断器
func ThreePhaseWatcher(watcher Watcher, recovery Recovery, unstableWatcher Watcher, stableWatcher Watcher) RecoverableWatcher {
	wr := NewWatcherChain(watcher, recovery)

	unstableChain := NewWatcherChain(unstableWatcher, recovery)
	unstableChain.ChainTo(wr)

	stableChain := NewWatcherChain(stableWatcher, EmptyRecovery{})
	stableChain.ChainTo(wr)

	wr.ChainTo(NewWatcherSelector([]RecoverableWatcher{
		unstableChain, stableChain,
	}))

	return wr
}

func SimpleThreePhaseBreaker(abnormalPerSecond float64, bucketCount int, minWaitOnOpen, maxWaitOnOpen time.Duration) *Breaker {
	watcher := NewRateWatcher(rate.NewLimiter(rate.Limit(abnormalPerSecond), bucketCount))
	recovery := NewDurationRecovery(minWaitOnOpen, maxWaitOnOpen)
	return New(ThreePhaseWatcher(watcher, recovery, NewCounterWatcher(1, true), NewCounterWatcher(1, false)))
}

type WatcherChain struct {
	watcher Watcher
	recovery Recovery
	next RecoverableWatcher
}

func NewWatcherChain(watcher Watcher, recovery Recovery) *WatcherChain {
	return &WatcherChain{
		watcher: watcher,
		recovery: recovery,
	}
}

func (ww *WatcherChain) Accept(abnormal bool) {
	ww.watcher.Accept(abnormal)
}

func (ww *WatcherChain) ChainTo(wc RecoverableWatcher) {
	ww.next = wc
}

func (ww *WatcherChain) WaitUnacceptable(ctx context.Context) WatchableRecovery {
	ww.watcher.WaitUnacceptable(ctx)
	if ctx.Err() == nil {
		return ww
	}
	return nil
}

func (ww WatcherChain) WaitRecovery(ctx context.Context) RecoverableWatcher {
	ww.recovery.WaitRecovery(ctx)
	if ctx.Err() == nil {
		return ww.next
	}
	return nil
}

// accept imply
type RateAcceptor struct {
	rw sync.RWMutex
	limiter *rate.Limiter
	threshold chan struct{}
}

func NewRateWatcher(limiter *rate.Limiter) *RateAcceptor {
	return &RateAcceptor{
		limiter: limiter,
		threshold: make(chan struct{}),
	}
}


func (acceptor *RateAcceptor) Accept(abnormal bool) {
	acceptor.rw.RLock()
	defer acceptor.rw.RUnlock()
	if abnormal && !acceptor.limiter.Allow() {
		select {
		case acceptor.threshold <- struct{}{}:
		default:
		}
	}
}

func (acceptor *RateAcceptor) WaitUnacceptable(ctx context.Context) {
		acceptor.rw.RLock()
		acceptor.limiter.SetLimit(acceptor.limiter.Limit())
		cn := acceptor.threshold
		acceptor.rw.RUnlock()

		select {
		case <- cn:
			acceptor.rw.Lock()
			acceptor.threshold = make(chan struct{})
			acceptor.rw.Unlock()
			return
		case <- ctx.Done():
			return
		}
}

type TimeRecovery struct {
	rw       sync.RWMutex
	maxDuration time.Duration
	minDuration time.Duration
	acceptor Watcher
}

func NewDurationRecovery(min, max time.Duration) *TimeRecovery {
	return &TimeRecovery{
		maxDuration: max,
		minDuration: min,
	}
}

func (recovery *TimeRecovery) WaitRecovery(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}

	delta := recovery.maxDuration - recovery.minDuration

	if delta <= 0 {
		recovery.rw.RLock()
		defer recovery.rw.RUnlock()
		return
	}

	delta = recovery.minDuration + time.Duration(rand.Int63n(int64(delta)))
	timer := time.NewTicker(delta)
	select {
	case <- ctx.Done():
		timer.Stop()
		return
	case <- timer.C:
		recovery.rw.RLock()
		defer recovery.rw.RUnlock()
		return
	}
}

type CounterWatcher struct {
	rw        sync.RWMutex
	count     int32
	abnormal  bool
	countDown int32
	cn        chan struct{}
}

func NewCounterWatcher(count int32, abnormal bool) *CounterWatcher {
	return &CounterWatcher{
		count:     count,
		countDown: count,
		abnormal:  abnormal,
		cn:        make(chan struct{}),
	}
}


func (watcher *CounterWatcher) Accept(abnormal bool) {
	var res int32

	if abnormal == watcher.abnormal {
		res = atomic.AddInt32(&watcher.countDown, -1)
	}

	if res <= 0 {
		select {
		case watcher.cn <- struct{}{}:
		default:
		}
	}
}

func (watcher *CounterWatcher) WaitUnacceptable(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}

	atomic.StoreInt32(&watcher.countDown, watcher.count)

	select {
	case <- ctx.Done():
		return
	case <- watcher.cn:
		return
	}
}



type WatcherSelector struct {
	watchers []RecoverableWatcher
}

func NewWatcherSelector(rw []RecoverableWatcher) *WatcherSelector {
	return &WatcherSelector{
		watchers: rw,
	}
}

func (selector *WatcherSelector) Accept(abnormal bool) {
	for _, s := range selector.watchers {
		s.Accept(abnormal)
	}
}

func (selector *WatcherSelector) WaitUnacceptable(ctx context.Context) WatchableRecovery {
	var cancel func()
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()
	cn := make(chan WatchableRecovery, 1)
	for _, w := range selector.watchers {
		go func(w RecoverableWatcher) {
			wr := w.WaitUnacceptable(ctx)
			if wr != nil {
				select {
				case cn <- wr:
				default:
				}
			}
		}(w)
	}

	select {
	case <- ctx.Done():
		return nil
	case wr := <- cn:
		return wr
	}
}

type EmptyRecovery struct {}

func (er EmptyRecovery) WaitRecovery(ctx context.Context) { }



