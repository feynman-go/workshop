package client

import (
	"context"
	"errors"
	"github.com/feynman-go/workshop/promise"
	"github.com/valyala/fastrand"
	"sync"
	"time"
)

const (
	MiddleNameWaitRetry = "wait-to-retry"
)

type DoOption struct {
	retry      func(err error) bool
	timeOut    time.Duration
	handler    func(ctx context.Context) error
	handleFail func(err error) error
}

type ExecuteResult struct {
	Try     int
	Recover int
	Err     error
}

type Client struct {
	promisePool  *promise.Pool
	resourcePool ResourcePool
}

func New(promisePool *promise.Pool, resourcePool ResourcePool) *Client {
	return &Client{
		promisePool, resourcePool,
	}
}

type Action struct {
	Partition    bool
	PartitionKey int
	Do           func(ctx context.Context, resource *Resource) error
	Recover      func(ctx context.Context, resource *Resource, err error) (wait time.Duration, ok bool)
	HandleFail   func(ctx context.Context, err error)
	MaxTimeOut   time.Duration // zero is useless
	MinTimeOut   time.Duration // zero is useless
}

func (manager *Client) Do(ctx context.Context, action Action) ExecuteResult {
	var er = &ExecuteResult{}

	process, opts := manager.buildProcess(action, er)
	p := promise.NewPromise(manager.promisePool, process, opts...)

	p.Recover(manager.buildRecoverProcess(action, er)).HandleException(manager.buildFailedProcess(action, er))
	_, err := p.Get(ctx, true)
	if err != nil {
		er.Err = err
	}
	return *er
}

func (manager *Client) buildProcess(action Action, er *ExecuteResult) (promise.ProcessFunc, []promise.Middle) {
	return func(ctx context.Context, request promise.Request) promise.Result {
			timeout := manager.getRandomTimeout(action)
			if timeout != 0 {
				ctx, _ = context.WithTimeout(ctx, timeout)
			}
			res, err := manager.resourcePool.Get(ctx, action.Partition, action.PartitionKey)
			if err != nil {
				return promise.Result{
					Err: ErrGetResource,
				}
			}
			if action.Do != nil {
				er.Try++
				err = action.Do(ctx, res)
			}
			if err == nil {
				res.PutBack(false)
			}
			return promise.Result{
				Err:     err,
				Payload: res,
			}
		}, []promise.Middle{
			promise.EventKeyMiddle(action.PartitionKey).WithInheritable(true),
			promise.PartitionMiddle(action.Partition).WithInheritable(true),
			promise.WaitMiddle(func(ctx context.Context, request promise.Request) error {
				payload, ok := request.LastPayload().(recoverPayload)
				if !ok {
					return nil
				}
				if payload.waitTime != 0 {
					return promise.WaitTime(ctx, payload.waitTime)
				}
				return nil
			}),
		}
}

type recoverPayload struct {
	waitTime time.Duration
}

func (manager *Client) buildRecoverProcess(action Action, er *ExecuteResult) promise.ProcessFunc {
	return func(ctx context.Context, request promise.Request) promise.Result {
		res, ok := request.LastPayload().(*Resource)
		if ok {
			defer func() {
				if res.Live() {
					res.PutBack(true)
				}
			}()
		}
		timeout := manager.getRandomTimeout(action)
		if timeout != 0 {
			ctx, _ = context.WithTimeout(ctx, timeout)
		}

		if action.Recover == nil { // do retry
			return promise.Result{
				Err: request.LastErr(),
			}
		}
		er.Recover++
		wait, ok := action.Recover(ctx, res, request.LastErr())
		if !ok {
			return promise.Result{
				Err: request.LastErr(),
			}
		}
		return promise.Result{
			Payload: recoverPayload{
				waitTime: wait,
			},
		}
	}
}

func (manager *Client) buildFailedProcess(action Action, er *ExecuteResult) promise.ProcessFunc {
	return func(ctx context.Context, request promise.Request) promise.Result {
		if action.HandleFail == nil {
			return promise.Result{
				Err: request.LastErr(),
			}
		}
		action.HandleFail(ctx, request.LastErr())
		return promise.Result{
			Err: request.LastErr(),
		}
	}
}
func (manager *Client) getRandomTimeout(action Action) time.Duration {
	return time.Duration(fastrand.Uint32n(uint32((action.MaxTimeOut-action.MinTimeOut)/time.Millisecond)) + uint32(action.MinTimeOut/time.Millisecond))
}

type ResourcePool interface {
	Get(ctx context.Context, partition bool, partitionKey int) (*Resource, error)
}

type Resource struct {
	v       interface{}
	recycle func(abnormal bool)
	rw      sync.RWMutex
	pusBack bool
}

func NewResource(recycle func(abnormal bool), v interface{}) *Resource {
	return &Resource{
		v:       v,
		recycle: recycle,
	}
}

func (res *Resource) Live() bool {
	res.rw.RLock()
	defer res.rw.RUnlock()
	if res.pusBack {
		return false
	}
	return true
}

func (res *Resource) Get() interface{} {
	res.rw.RLock()
	defer res.rw.RUnlock()
	if res.pusBack {
		return nil
	}
	return res.v
}

func (res *Resource) PutBack(abnormal bool) {
	res.rw.RLock()
	defer res.rw.RUnlock()
	if res.pusBack {
		return
	}
	if res.recycle != nil {
		res.recycle(abnormal)
	}
	res.v = nil
	res.pusBack = true
}

var ErrGetResource = errors.New("get resource err")
var ErrResourceAbnormal = errors.New("get resource abnormal")

/**
help function to build default function

*/
const (
	DefaultMinTimeOut = 500 * time.Millisecond
	DefaultMaxTimeOut = time.Second
)

func DefaultAction(do func(ctx context.Context, resource *Resource) error) Action {
	var maxCount int
	action := Action{
		Partition:    false,
		PartitionKey: 1,
		Do:           do,
		Recover: func(ctx context.Context, resource *Resource, err error) (time.Duration, bool) {
			if resource != nil {
				resource.PutBack(true)
			}
			maxCount++
			if maxCount >= 3 {
				return 0, false
			}
			return time.Second, true
		},
		MaxTimeOut: DefaultMaxTimeOut,
		MinTimeOut: DefaultMinTimeOut,
	}
	return action
}
