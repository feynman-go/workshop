package promise

import (
	"context"
	"sync"
)

type Request struct {
	from     *processInstance
	Wait      func(ctx context.Context, req Request) error
	Partition bool
	EventKey  int
	Process   ProcessFunc
	MiddleEnd bool
}

func (r Request) LastErr() error {
	if r.from != nil && r.from.Result != nil {
		return r.from.Result.Err
	}
	return nil
}

func (r Request) LastPayload() interface{} {
	if r.from != nil && r.from.Result != nil {
		return r.from.Result.Payload
	}
	return nil
}

type processInstance struct {
	Req    Request
	Result *Result
}

type Result struct {
	Err error
	Payload interface{}
}

type ProcessFunc func(ctx context.Context, req Request) Result

type Promise struct {
	success   *Promise
	exception *Promise
	pool      *Pool
	process   ProcessFunc
	middles   *MiddleLink
	*chanStatus
}

func NewPromise(pool *Pool, process ProcessFunc, middles ...Middle) *Promise {
	return newPromise(process, new(MiddleLink).Append(middles...), pool)
}

// only on success, return a new Process, basic interface
func (p *Promise) Then(ps ProcessFunc, middles ...Middle) *Promise {
	return p.setNext(ps, middles, true)
}

// return new exception future, basic interface
func (p *Promise) OnException(ps ProcessFunc, middles ...Middle) *Promise {
	return p.setNext(ps, middles, false)
}

type ExceptionPromise struct {
	p *Promise
}

func (p ExceptionPromise) HandleException(ps ProcessFunc, middles ...Middle) *Promise {
	return p.p.OnException(ps, middles...)
}

func (p *Promise) TryRecover(recover ProcessFunc, onRecoverFailed ProcessFunc, middles ...Middle) *Promise {
	p.Recover(recover, middles...).HandleException(onRecoverFailed)
	return p
}

func (p *Promise) Recover(recover ProcessFunc, middles ...Middle) ExceptionPromise {
	recoverPromise := p.setNextPromise(func() *Promise{
		return fromPromise(p, recover, middles)
	}, false)

	recoverPromise.setNextPromise(func() *Promise{
		return p
	}, true)

	return ExceptionPromise{recoverPromise}
}

func (p *Promise) Get(ctx context.Context, close bool) (interface{}, error) {
	p.Start(ctx)
	if close {
		defer p.Close()
	}
	select {
	case <- ctx.Done():
		return nil, ctx.Err()
	case <- p.closeChan:
		var err error
		var v *Result
		//Need sync. Because of golang memory modal.
		p.mu.RLock()
		v, err = p.chanStatus.getResult()
		p.mu.RUnlock()
		if err != nil {
			return nil, err
		}
		if v == nil {
			return nil, nil
		}
		return v.Payload, err
	}
}

func (p *Promise) IsClosed() bool {
	return p.chanStatus.isClosed()
}

// Wait all sub route finish or closed
func (p *Promise) Wait(ctx context.Context, close bool) error {
	if close {
		defer p.Close()
	}
	p.Start(ctx)
	err := p.waitClose(ctx)
	return err
}

// Try start, if chan is started or closed return false, other return true
func (p *Promise) Start(ctx context.Context) bool {
	return p.chanStatus.tryStart(ctx)
}

// close will close all parent or sub future
func (p *Promise) Close() {
	p.chanStatus.close(nil, nil)
}

func (p *Promise) setNextPromise(buildNext func() *Promise, success bool) *Promise {
	var ret *Promise
	ok := p.tryUnStart(func() {
		ret = buildNext()
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

func (p *Promise) setNext(ps ProcessFunc, middles []Middle, success bool) *Promise {
	return p.setNextPromise(func() *Promise{
		return fromPromise(p, ps, middles)
	}, success)
}

func (p *Promise) newTaskBox(req Request, taskFunc TaskFunc) TaskBox {
	task := TaskBox {
		closed: p.closeChan,
		f:      taskFunc,
	}

	if req.Partition {
		task.stubborn = true
		task.localId = int(req.EventKey)
	}

	return task
}

func (p *Promise) post(ctx context.Context, lastProcess *processInstance) error {
	req := Request{
		from:     lastProcess,
		Process: p.process,
	}
	go func() {
		ctx, cancel := context.WithCancel(context.Background())
		var err error
		if ms := p.middles; ms != nil {
			ms.Range(func(middle Middle) bool {
				if middle.Wrapper != nil {
					req = middle.Wrapper(req)
				}
				return !req.MiddleEnd
			})
		}
		if req.Wait != nil { //Wait func
			go func() {
				select {
				case <- p.getCloseChan():
					cancel()
				case <- ctx.Done():
				}
			}()
			err = req.Wait(ctx, req)
		}
		if err == nil {
			err = p.pool.Feed(ctx, p.newTaskBox(req, func(ctx context.Context, localId int) {
				p.doProcess(ctx, req)
			}))
		}
		cancel()
		if err != nil {
			p.close(err, lastProcess)
		}
	}()
	return nil
}

func (p *Promise) doProcess(ctx context.Context, req Request) {
	instance := &processInstance{
		Req: req,
	}

	var err = req.LastErr()
	if req.Process != nil {
		result := req.Process(ctx, req)
		instance.Result = &result
		err = instance.Result.Err
	}

	if err != nil {
		if exception := p.exception; exception != nil {
			err = exception.post(ctx, instance)
			if err != nil {
				p.chanStatus.close(err, instance)
			}
		} else {
			p.chanStatus.close(err, instance)
		}
		return
	}
	next := p.success
	if next == nil {
		p.close(err, instance)
		return
	}
	err = next.post(ctx, instance)
	if err != nil {
		p.chanStatus.close(err, instance)
	}
}


func fromPromise(from *Promise, process ProcessFunc, middles []Middle) *Promise {
	p := new(Promise)
	p.pool = from.pool
	p.process = process
	p.chanStatus = from.chanStatus
	var ms = from.middles
	if ms == nil {
		ms = new(MiddleLink)
	}
	p.middles = ms.IncFragmentID().Append(middles...)
	return p
}

func newPromise(process ProcessFunc, link *MiddleLink, pool *Pool) *Promise {
	p := new(Promise)
	p.pool = pool
	p.middles = link
	p.process = process
	var st = new(chanStatus)
	st.err = new(error)
	st.closeChan = make(chan struct{}, 0)
	st.started = false
	st.root = p
	p.chanStatus = st
	return p
}

type chanStatus struct {
	started bool
	closeChan chan struct{}
	lastInstance *processInstance
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
func (s *chanStatus) tryStart(ctx context.Context) bool {
	if ctx.Err() != nil {
		return false
	}
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
	err := s.root.post(ctx, nil)
	s.mu.Unlock()
	if err != nil {
		s.close(err, nil)
	}
	return true
}

func (s *chanStatus) getResult() (*Result, error) {
	var err error
	s.mu.RLock()
	res := s.lastInstance
	err = *s.err
	s.mu.RUnlock()

	if res == nil {
		return nil, err
	}
	return res.Result, err
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

func (s *chanStatus) close(err error, instance *processInstance) {
	s.mu.Lock()
	select {
	case <- s.closeChan:
	default:
		close(s.closeChan)
		*s.err = err
		s.lastInstance = instance
	}
	s.mu.Unlock()
}

func (s *chanStatus) getCloseChan() <- chan struct{} {
	return s.closeChan
}
