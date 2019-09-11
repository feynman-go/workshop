package parallel

import (
	"context"
	"testing"
	"time"
)

func TestParallelSelfClose(t *testing.T) {
	var closed1 = make(chan struct{})
	var closed2 = make(chan struct{})
	var closed3 = make(chan struct{})
	RunParallel(context.Background(), func(ctx context.Context) {
		time.Sleep(time.Second)
		close(closed1)
		return
	}, func(ctx context.Context) {
		select {
		case <- ctx.Done():
			close(closed2)
		}
	}, func(ctx context.Context) {
		select {
		case <- ctx.Done():
			close(closed3)
		}
	})

	select {
	case <- closed1:
	default:
		t.Fatal("closed1 should closed")
	}

	select {
	case <- closed2:
	default:
		t.Fatal("closed2 should closed")
	}

	select {
	case <- closed3:
	default:
		t.Fatal("closed3 should closed")
	}
}

func TestParallelContextClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var closed1 = make(chan struct{})
	var closed2 = make(chan struct{})
	var closed3 = make(chan struct{})
	go func() {
		RunParallel(ctx, func(ctx context.Context) {
			select {
			case <- ctx.Done():
				close(closed1)
			}
		}, func(ctx context.Context) {
			select {
			case <- ctx.Done():
				close(closed2)
			}
		}, func(ctx context.Context) {
			select {
			case <- ctx.Done():
				close(closed3)
			}
		})
	}()

	time.Sleep(time.Second)
	cancel()

	time.Sleep(time.Millisecond * 100)

	select {
	case <- closed1:
	default:
		t.Fatal("closed1 should closed")
	}

	select {
	case <- closed2:
	default:
		t.Fatal("closed2 should closed")
	}

	select {
	case <- closed3:
	default:
		t.Fatal("closed3 should closed")
	}
}

func TestParallelWaitClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var closed1 = make(chan struct{})
	var closed2 = make(chan struct{})
	var closed3 = make(chan struct{})
	go func() {
		RunParallel(ctx, func(ctx context.Context) {
			select {
			case <- ctx.Done():
				time.Sleep(time.Second)
				close(closed1)
			}
		}, func(ctx context.Context) {
			select {
			case <- ctx.Done():
				close(closed2)
			}
		}, func(ctx context.Context) {
			select {
			case <- ctx.Done():
				close(closed3)
			}
		})
	}()

	time.Sleep(time.Millisecond * 100)
	cancel()

	time.Sleep(time.Millisecond * 500)

	select {
	case <- closed1:
		t.Fatal("closed1 should not closed")
	default:
		time.Sleep(time.Millisecond * 600)
	}


	select {
	case <- closed1:
	default:
		t.Fatal("closed1 should closed")
	}

	select {
	case <- closed2:
	default:
		t.Fatal("closed2 should closed")
	}

	select {
	case <- closed3:
	default:
		t.Fatal("closed3 should closed")
	}
}