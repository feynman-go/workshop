package promise

import (
	"context"
	"sync"
	"time"
)

type Middle struct {
	Name        string
	Wrapper     func(process Request) Request
	Inheritable bool
}

func (mid Middle) WithInheritable(inheritable bool) Middle {
	mid.Inheritable = inheritable
	return mid
}

func (mid Middle) WithName(name string) Middle {
	mid.Name = name
	return mid
}

func WrapProcess(name string, wrapper func(context.Context, Request, ProcessFunc) Result) Middle {
	return Middle{
		Name: name,
		Wrapper: func(request Request) Request {
			p := request.Process
			request.Process = func(ctx context.Context, req Request) Result {
				return wrapper(ctx, req, p)
			}
			return request
		},
	}
}

func WrapTimeout(name string, timeout time.Duration) Middle {
	return WrapProcess(name, func(ctx context.Context, req Request, p ProcessFunc) Result {
		ctx, _ = context.WithTimeout(ctx, timeout)
		return p(ctx, req)
	})
}

func WaitMiddle(wait func(ctx context.Context, req Request) error) Middle {
	return Middle{
		Wrapper: func(request Request) Request {
			request.Wait = wait
			return request
		},
	}
}

func WaitTime(ctx context.Context, waitTime time.Duration) error {
	if waitTime == 0 {
		return nil
	}
	t := timerPool.Get().(*time.Timer)
	if !t.Stop() {
		<-t.C
	}
	t.Reset(waitTime)
	select {
	case <-t.C:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func WaitTimeMiddle(waitTime time.Duration) Middle {
	return WaitMiddle(func(ctx context.Context, _ Request) error {
		return WaitTime(ctx, waitTime)
	})
}

func PartitionMiddle(partition bool) Middle {
	return Middle{
		Wrapper: func(request Request) Request {
			request.Partition = partition
			return request
		},
	}
}

func EventKeyMiddle(eventKey int) Middle {
	return Middle{
		Wrapper: func(request Request) Request {
			request.EventKey = eventKey
			return request
		},
	}
}

// help
var timerPool = &sync.Pool{
	New: func() interface{} {
		return time.NewTimer(0)
	},
}

type MiddleLink struct {
	root      *MiddleLink
	fId       int
	next      *MiddleLink
	inherited *Middle
	inner     []Middle
}

func (link *MiddleLink) IncFragmentID() *MiddleLink {
	if link.root == nil {
		link.root = link
	}
	ml := *link
	ml.fId++
	return &ml
}

func (link *MiddleLink) Append(md ...Middle) *MiddleLink {
	var ret = link
	for _, m := range md {
		ret = ret.append(m)
	}
	return link
}

func (link *MiddleLink) append(md Middle) *MiddleLink {
	// init root
	if link.root == nil {
		link.root = link
	}

	for link.next != nil {
		link = link.next
	}
	if md.Inheritable {
		newLink := *link
		newLink.inherited = &md
		link.next = &newLink
		return &newLink
	} else {
		link.inner = append(link.inner, md)
		return link
	}
}

func (link *MiddleLink) Range(walk func(middle Middle) bool) {
	if link.root == nil {
		link.root = link
	}
	var ctn = true
	cur := link.root
	for cur != nil && ctn {
		if cur.inherited != nil {
			ctn = walk(*cur.inherited)
		}
		for i := 0; cur.fId == link.fId && ctn && i < len(cur.inner); i++ {
			ctn = walk(cur.inner[i])
		}
		cur = cur.next
	}
}
