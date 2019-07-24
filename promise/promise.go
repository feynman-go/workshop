package promise

import (
	"container/list"
	"context"
	"sync"
	"time"
)

type Request struct {
	from    *processInstance
	Process ProcessFunc
	profiles *profiles
}

func (req *Request) WithOption(opt ...Option) {
	req.profiles = req.profiles.withOpts(opt)
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

type Middles struct {
	list *list.List
	rw   sync.RWMutex
}

const (
	PlaceHoldMiddleName = "__place_hold"
)

type MiddleWrapper struct {
	l       *list.List
	element *list.Element
}

type Middle struct {
	Name        string
	Wrapper     func(process Request) (Request, bool)
	placeholder bool
}

func (wrapper MiddleWrapper) Name() string {
	return wrapper.element.Value.(Middle).Name
}

func (wrapper MiddleWrapper) InsertBefore(middle Middle) {
	wrapper.l.InsertBefore(middle, wrapper.element)
}

func (wrapper MiddleWrapper) InsertAfter(middle Middle) {
	wrapper.l.InsertAfter(middle, wrapper.element)
}

func (wrapper MiddleWrapper) Remove() {
	wrapper.l.Remove(wrapper.element)
}

func (wrapper MiddleWrapper) IsHead() bool {
	return wrapper.element.Prev() == nil
}

func (wrapper MiddleWrapper) IsTail() bool {
	return wrapper.element.Next() == nil
}

func (wrapper MiddleWrapper) IsPlaceholder() bool {
	return wrapper.element.Value.(Middle).placeholder
}

func (wrapper *MiddleWrapper) Update(opt Middle) {
	wrapper.element.Value = opt
}

func (middles *Middles) getList() *list.List {
	middles.rw.Lock()
	defer middles.rw.Unlock()
	if middles.list == nil {
		middles.list = list.New()
	}
	return middles.list
}

// walk middlesFactory and return new middlesFactory
func (middles *Middles) Walk(walker func(m MiddleWrapper) bool) *Middles {
	var (
		ctn = true
		ls = list.New()
		wrapper = MiddleWrapper {
			l: ls,
		}
	)
	// copy TODO may to be improved
	cur := middles.getList().Front()
	for cur != nil {
		ls.PushBack(cur.Value)
		cur = cur.Next()
	}

	var placeHold *list.Element
	if ls.Front() == nil {
		placeHold = ls.PushBack(Middle{
			Name:        PlaceHoldMiddleName,
			placeholder: true,
		})
	}

	cur = ls.Back()
	for cur != nil {
		prev := cur.Prev()
		wrapper.element = cur
		if ctn {
			ctn = walker(wrapper)
		}
		cur = prev
	}
	if placeHold != nil {
		ls.Remove(placeHold)
	}

	return &Middles{
		list: ls,
	}
}

func (middles *Middles) Append(middle ...Middle) *Middles {
	return middles.Walk(func(m MiddleWrapper) bool {
		if m.IsTail() {
			for _, md := range middle {
				m.InsertAfter(md)
			}
			return false
		}
		return true
	})
}

func (middles *Middles) warp(in Request) (out Request) {
	var (
		ok = true
		ls = middles.getList()
		cur = ls.Front()
	)


	out = in
	for cur != nil && ok {
		md := cur.Value.(Middle)
		if md.Wrapper != nil {
			out, ok = md.Wrapper(out)
		}
		cur = cur.Next()
	}
	return out
}

type Promise struct {
	eventKey  int64
	//from      *Promise
	process   ProcessFunc
	success   *Promise
	exception *Promise
	pool      *Pool
	profiles  *profiles
	*chanStatus
}

func NewPromise(pool *Pool, process ProcessFunc, opt ...Option) *Promise {
	p := new(profiles)
	if len(opt) > 0 {
		p = p.withOpts(opt)
	}
	return newPromise(process, p, pool)
}

// only on success, return a new Process, basic interface
func (p *Promise) Then(ps ProcessFunc, option ...Option) *Promise {
	return p.setNext(ps, option, true)
}

// return new exception future, basic interface
func (p *Promise) OnException(ps ProcessFunc, option ...Option) *Promise {
	return p.setNext(ps, option, false)
}

type ExceptionPromise struct {
	p *Promise
}

func (p ExceptionPromise) HandleException(ps ProcessFunc, option ...Option) *Promise {
	return p.p.OnException(ps, option...)
}

func (p *Promise) TryRecover(recover ProcessFunc, onRecoverFailed ProcessFunc, option ...Option) *Promise {
	p.Recover(recover, option...).HandleException(onRecoverFailed)
	return p
}

func (p *Promise) Recover(recover ProcessFunc, option ...Option) ExceptionPromise {
	recoverPromise := p.setNextPromise(func() *Promise{
		return fromPromise(p, recover, option)
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

// wait all sub route finish or closed
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

func (p *Promise) setNext(ps ProcessFunc, opts []Option, success bool) *Promise {
	return p.setNextPromise(func() *Promise{
		return fromPromise(p, ps, opts)
	}, success)
}

func (p *Promise) newTaskBox(taskFunc TaskFunc) TaskBox {
	task := TaskBox {
		closed: p.closeChan,
		f:      taskFunc,
	}

	if p.profiles.partition {
		task.stubborn = true
		task.localId = int(p.eventKey)
	}

	return task
}

func (p *Promise) post(ctx context.Context, lastProcess *processInstance) error {
	req := Request{
		from:    lastProcess,
		Process: p.process,
		profiles: p.profiles,
	}
	go func() {
		ctx, cancel := context.WithCancel(context.Background())
		var err error
		if ms := p.profiles.middles; ms != nil {
			req = ms.warp(req)
		}
		if req.profiles.waitTime != 0 { //wait time
			t := timerPool.Get().(*time.Timer)
			if !t.Stop() {
				<-t.C
			}
			t.Reset(req.profiles.waitTime)
			select {
			case <- t.C:
			case <- p.getCloseChan():
				cancel()
			case <- ctx.Done():
				err = ctx.Err()
			}
		}
		if err == nil {
			err = p.pool.Feed(ctx, p.newTaskBox(func(ctx context.Context, localId int) {
				p.doProcess(ctx, req)
			}))
		}
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


func fromPromise(from *Promise, process ProcessFunc, opts []Option) *Promise {
	p := new(Promise)
	p.pool = from.pool
	p.process = process
	p.chanStatus = from.chanStatus
	p.profiles = from.profiles.withOpts(opts)
	return p
}

func newPromise(process ProcessFunc, ps *profiles, pool *Pool) *Promise {
	p := new(Promise)
	p.pool = pool
	p.process = process
	p.profiles = ps
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

var timerPool = &sync.Pool{
	New: func() interface{} {
		return time.NewTimer(0)
	},
}

type Option struct {
	partition *bool
	eventKey  *int
	waitTime *time.Duration
	middles *func(middles *Middles) *Middles
}

type profiles struct {
	middles *Middles
	waitTime time.Duration
	partition bool
	eventKey int
}

func (from *profiles) withOpts(option []Option) *profiles{
	to := new(profiles)
	*to = *from

	for _, opt := range option {
		if opt.waitTime != nil {
			to.waitTime = *opt.waitTime
		}
		if opt.partition != nil {
			to.partition = *opt.partition
		}
		if opt.eventKey != nil {
			to.eventKey = *opt.eventKey
		}
		if opt.middles != nil {
			if to.middles == nil {
				to.middles = &Middles{}
			}
			to.middles = (*opt.middles)(to.middles)
		}
	}
	return to
}

func WithPartition(partition bool) Option {
	return Option{
		partition: &partition,
	}
}

func WithEventKey(eventKey int) Option {
	return Option{
		eventKey: &eventKey,
	}
}

func WithWaitTime(duration time.Duration) Option {
	return Option{
		waitTime: &duration,
	}
}

func WithAppendMiddles(middle ...Middle) Option {
	f := func(ms *Middles) *Middles {
		return ms.Append(middle...)
	}
	return Option {
		middles: &f,
	}
}

func WithRemoveMiddles(all bool, names ...string) Option {
	f := func(ms *Middles) *Middles {
		return ms.Walk(func(wrapper MiddleWrapper) bool {
			for _, n := range names {
				if n != wrapper.Name() {
					continue
				}
				wrapper.Remove()
				if !all {
					return false
				}
			}
			return true
		})
	}
	return Option {
		middles: &f,
	}
}


func WrapProcess(name string, wrapper func(context.Context, Request, ProcessFunc) Result) Middle {
	return Middle{
		Name: name,
		Wrapper: func(request Request) (Request, bool) {
			p := request.Process
			request.Process = func(ctx context.Context, req Request) Result{
				return wrapper(ctx, req, p)
			}
		},
	}
}

func WrapTimeout(name string, timeout time.Duration) Middle {
	return WrapProcess(name, func(ctx context.Context, req Request, p ProcessFunc) Result {
		ctx, _ = context.WithTimeout(ctx, timeout)
		return p(ctx, req)
	})
}
