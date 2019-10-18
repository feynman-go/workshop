package promise

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestPoolParallelFeedBlock(t *testing.T) {
	p := NewPool(2)
	var rw sync.RWMutex
	var m = map[int]int{}
	var group = &sync.WaitGroup{}
	group.Add(100)
	time.Sleep(30 * time.Millisecond)
	for i := 0 ; i < 100; i ++{
		go func(i int) {
			start := time.Now()
			p.Feed(context.Background(), TaskBox{
				closed: make(chan struct{}),
				f: TaskFunc(func(ctx context.Context, localId int) {
					time.Sleep(100 * time.Millisecond)
				}),
			})
			key := int(time.Now().Sub(start) / (100 * time.Millisecond))
			rw.Lock()
			m[key] += 1
			rw.Unlock()
			group.Done()
		}(i)

	}
	group.Wait()
	for k, v :=  range m {
		t.Log("key:", k, "\tcount:", v)
	}

}

func TestPoolParallelFeedControlGroup(t *testing.T) {
	var rw sync.RWMutex
	var m = map[int]int{}
	var group = &sync.WaitGroup{}
	group.Add(100)

	var chan1 = make(chan (chan struct{}), 1)

	go func() {
		for callback := range chan1 {
			time.Sleep(100 * time.Millisecond)
			close(callback)
		}
	}()

	go func() {
		for callback := range chan1 {
			time.Sleep(100 * time.Millisecond)
			close(callback)
		}
	}()

	for i := 0 ; i < 100; i ++{
		time.Sleep(30 * time.Millisecond)
		go func() {
			start := time.Now()
			callback := make(chan struct{})
			chan1 <- callback
			<- callback

			key := int(time.Now().Sub(start) / (100 * time.Millisecond))
			rw.Lock()
			m[key] += 1
			rw.Unlock()
			group.Done()
		}()

	}
	group.Wait()
	for k, v :=  range m{
		t.Log("key:", k, "\tcount:", v)
	}

}
