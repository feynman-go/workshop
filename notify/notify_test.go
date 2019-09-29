package notify

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

type MockPublisher struct {
	PublishFunc func(ctx context.Context, message []Message) string
}

func (publisher MockPublisher) Publish(ctx context.Context, message []Message) (lastToken string) {
	if publisher.PublishFunc != nil {
		return publisher.PublishFunc(ctx, message)
	}
	return ""
}

func TestNotifySend(t *testing.T) {
	var count int32
	stream := NewMemoMessageStream()
	notifier := New(stream, stream, MockPublisher{
		PublishFunc: func(ctx context.Context, messages []Message) string {
			atomic.AddInt32(&count, 1)
			return messages[0].Token
		},
	}, Option{})
	defer notifier.Close()

	time.Sleep(100 * time.Millisecond)

	stream.Push(Message{
		ID: "1",
	})

	time.Sleep(100 * time.Millisecond)
	if count != 1 {
		t.Fatal("fatal count message")
	}
}

func TestNotifySendMulti(t *testing.T) {
	var count int32
	stream := NewMemoMessageStream()
	notifier := New(stream, stream, MockPublisher{
		PublishFunc: func(ctx context.Context, messages []Message) string {
			atomic.AddInt32(&count, 1)
			return messages[0].Token
		},
	}, Option{})
	defer notifier.Close()

	time.Sleep(100 * time.Millisecond)

	stream.Push(Message{
		ID: "1",
	})

	stream.Push(Message{
		ID: "2",
	})

	time.Sleep(100 * time.Millisecond)
	time.Sleep(time.Second)
	if count != 2 {
		t.Fatal("fatal count message")
	}
}

func TestNotifyOneByOne(t *testing.T) {
	var count int32
	stream := NewMemoMessageStream()
	notifier := New(stream, stream, MockPublisher{
		PublishFunc: func(ctx context.Context, messages []Message) string {
			atomic.AddInt32(&count, 1)
			return messages[0].Token
		},
	}, Option{})
	defer notifier.Close()

	stream.Push(Message{
		ID: "1",
	})

	time.Sleep(100 * time.Millisecond)
	if atomic.LoadInt32(&count) != 1 {
		t.Fatal("fatal count message")
	}

	stream.Push(Message{
		ID: "2",
	})

	time.Sleep(100 * time.Millisecond)

	if atomic.LoadInt32(&count) != 2 {
		t.Fatal("fatal count message")
	}

	stream.Push(Message{
		ID: "1",
	})

	time.Sleep(100 * time.Millisecond)

	if atomic.LoadInt32(&count) != 3 {
		t.Fatal("fatal count message")
	}
}