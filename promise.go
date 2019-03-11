package workshop

import (
	"context"
	"sync"
)

type ProcessFunc func(ctx context.Context, last interface{}) (interface{}, error)
type ExceptionProcessFunc func(ctx context.Context, err error, last interface{}) (interface{}, error)

type Promise struct {
	eventKey  int64
	//from      *Promise
	process   Process
	success   *Promise
	exception *Promise
	pool      *Pool
	*chanStatus
}

func NewPromise(pool *Pool, process Process) *Promise {
	return newPromise(process, pool)
}

type Process struct {
	EventKey int
	Process ProcessFunc
	Middles []Middle
	Append bool // override or append
}

type errData struct {
	data interface{}
	err error
}


type ExceptionProcess struct {
	EventKey int
	Process ExceptionProcessFunc
	Middles []Middle
	Append bool // override or append
}

func (fp ExceptionProcess) process(ctx context.Context, last interface{}) (interface{}, error) {
	ed := last.(errData)
	return fp.Process(ctx, ed.err, last)
}

func (fp ExceptionProcess) toProcess() Process {
	return Process{
		fp.EventKey,
		fp.process,
		fp.Middles,
		fp.Append,
	}
}

// only on success, return a new process, basic interface
func (p *Promise) Then(ps Process) *Promise {
	return p.setNext(ps, true)
}

// return new exception future, basic interface
func (p *Promise) OnException(ep ExceptionProcess) *Promise {
	return p.setNext(ep.toProcess(), false)
}

// return f
func (p *Promise) RecoverAndRetry(recover ExceptionProcess) *Promise {
	var recoverPromise *Promise

	ok := p.tryUnStart(func() {
		recoverPromise = fromPromise(p, recover.toProcess())
		p.exception = recoverPromise
		recoverPromise.success = p
	})

	if !ok {
		if p.chanStatus.isStarted() {
			panic("promise chan has started")
		}
		if p.chanStatus.isClosed() {
			panic("promise chan has closed")
		}
		panic("config exception")
	}
	return p
}

func (p *Promise) Get(ctx context.Context) (interface{}, error) {
	p.Start()

	select {
	case <- ctx.Done():
		return nil, ctx.Err()
	case <- p.closeChan:
		var err error
		var v interface{}
		//Need sync. Because of golang memory modal.
		p.mu.RLock()
		v = *p.chanStatus.result
		err = *p.chanStatus.err
		p.mu.RUnlock()
		return v, err
	}
}

func (p *Promise) IsClosed() bool {
	return p.chanStatus.isClosed()
}

// wait all sub route finish or closed
func (p *Promise) Wait(ctx context.Context) error {
	p.Start()
	return p.waitClose(ctx)
}

// Try start, if chan is started or closed return false, other return true
func (p *Promise) Start() bool {
	return p.chanStatus.tryStart()
}

// close will close all parent or sub future
func (p *Promise) Close() error {
	p.chanStatus.close(nil, nil)
	return nil
}

func (p *Promise) setNext(ps Process, success bool) *Promise {
	var ret *Promise
	ok := p.tryUnStart(func() {
		ret = fromPromise(p, ps)
		if success {
			p.success = ret
		} else {
			p.exception = ret
		}
	})

	if !ok {
		if p.chanStatus.isStarted() {
			panic("promise chan has started")
		}
		if p.chanStatus.isClosed() {
			panic("promise chan has closed")
		}
		panic("config exception")
	}
	return ret
}

func (p *Promise) newTaskBox(taskFunc TaskFunc) taskBox {
	return taskBox{
		closed: p.closeChan,
		f:      taskFunc,
	}
}

func (p *Promise) post(last interface{}) error {
	dp := p.doProcess
	return p.pool.feedAnyway(p.newTaskBox(func(ctx context.Context) {
		dp(ctx, last)
	}))
}

func (p *Promise) beforeProcess(ctx context.Context, afterList []func(err error)) error {
	var err error
	var after func(error)
	for _, m := range p.process.Middles {
		if after, err = m.Before(p.eventKey); err != nil {
			return err
		}
		if after != nil {
			afterList = append(afterList, after)
		}
	}

	return nil
}

func (p *Promise) afterProcess(ctx context.Context, inErr error, list []func(error) ) {
	for _, after := range list {
		after(inErr)
	}
}

func (p *Promise) doProcess(ctx context.Context, last interface{}) {
	var v interface{}
	afterList := afterPool.Get().([]func(error))
	err := p.beforeProcess(ctx, afterList)
	if err == nil {
		v, err = p.process.Process(ctx, last)
		p.afterProcess(ctx, err, afterList)
	}

	afterPool.Put(afterList[:0])

	if err != nil {
		if exception := p.exception; exception != nil {
			err = exception.post(errData{
				last, err,
			})
			if err != nil {
				p.chanStatus.close(err, v)
			}
		} else {
			p.chanStatus.close(err, v)
		}
		return
	}
	next := p.success
	if next == nil {
		p.close(err, v)
		return
	}
	err = next.post(v)
	if err != nil {
		p.chanStatus.close(err, v)
	}
}


func fromPromise(from *Promise, process Process) *Promise {
	p := new(Promise)
	p.pool = from.pool
	p.process = process
	p.chanStatus = from.chanStatus
	if process.Middles == nil {
		p.process.Middles = from.process.Middles
	} else {
		if from.process.Middles == nil {
			p.process.Middles = append(([]Middle)(nil), process.Middles...)
		}
		if process.Append && from.process.Middles != nil {
			p.process.Middles = append(p.process.Middles, from.process.Middles...)
		}
	}
	return p
}

func newPromise(process Process, pool *Pool) *Promise {
	p := new(Promise)
	p.pool = pool
	p.process = process

	var st = new(chanStatus)

	st.result = new(interface{})
	st.err = new(error)
	st.closeChan = make(chan struct{}, 0)
	st.started = false
	st.root = p
	p.chanStatus = st
	return p
}

type Middle interface {
	Before(key int64) (After func(err error), err error) // before invoke
}

type chanStatus struct {
	started bool
	closeChan chan struct{}
	result    *interface{}
	err       *error
	root 	  *Promise
	mu sync.RWMutex
}

func (s *chanStatus) tryUnStart(f func()) bool {
	s.mu.RLock()
	if s.started || s.isClosed() {
		s.mu.RUnlock()
		return false
	}
	f()
	s.mu.RUnlock()
	return true
}

func (s *chanStatus) isStarted() bool {
	var ret bool
	s.mu.RLock()
	ret = s.started
	s.mu.RUnlock()
	return ret
}

// return already or closed
func (s *chanStatus) tryStart() bool {
	s.mu.Lock()
	if s.isClosed() {
		s.mu.Unlock()
		return false
	}
	if s.started {
		s.mu.Unlock()
		return false
	}
	s.started = true
	err := s.root.post(nil)
	s.mu.Unlock()
	if err != nil {
		s.close(err, nil)
	}
	return true
}

func (s *chanStatus) getResult() (interface{}, error) {
	var res interface{}
	var err error
	s.mu.RLock()
	res = *s.result
	err = *s.err
	s.mu.RUnlock()
	return res, err
}

func (s *chanStatus) isClosed() bool {
	select {
	case <- s.closeChan:
		return true
	default:
		return false
	}
}

func (s *chanStatus) waitClose(ctx context.Context) error {
	select {
	case <- s.closeChan:
		if s.err != nil {
			return *s.err
		}
		return nil
	case <- ctx.Done():
		return ctx.Err()
	}
}

func (s *chanStatus) close(err error, value interface{}) {
	s.mu.Lock()
	select {
	case <- s.closeChan:
	default:
		close(s.closeChan)
		*s.err = err
		*s.result = value
	}
	s.mu.Unlock()
}

var afterPool = &sync.Pool{ //receive memory of after list
	New: func() interface{}{
		return make([]func(err error), 0, 2)
	},
}