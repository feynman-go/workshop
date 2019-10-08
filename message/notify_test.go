package message

import (
	"context"
	"log"
	"sync/atomic"
	"testing"
	"time"
)

var _ Publisher = (*MockPublisher)(nil)

type MockPublisher struct {
	PublishFunc func(ctx context.Context, message []OutputMessage) error
}

func (publisher MockPublisher) Publish(ctx context.Context, message []OutputMessage) error {
	if publisher.PublishFunc != nil {
		return publisher.PublishFunc(ctx, message)
	}
	return nil
}

func TestNotifySend(t *testing.T) {
	var count int32
	stream := NewMemoMessageStream()
	notifier := New(stream, MockPublisher{
		PublishFunc: func(ctx context.Context, messages []OutputMessage) error {
			atomic.AddInt32(&count, 1)
			return nil
		},
	}, Option{})
	defer notifier.Close()

	time.Sleep(100 * time.Millisecond)

	stream.Push("topic", Message{
		UID: "1",
	})

	time.Sleep(100 * time.Millisecond)
	if count != 1 {
		t.Fatal("fatal count message")
	}
}

func TestNotifySendMulti(t *testing.T) {
	var count int32
	stream := NewMemoMessageStream()
	notifier := New(stream, MockPublisher{
		PublishFunc: func(ctx context.Context, messages []OutputMessage) error {
			log.Println("add count", atomic.AddInt32(&count, 1), time.Now())
			return nil
		},
	}, Option{})
	defer notifier.Close()

	time.Sleep(100 * time.Millisecond)

	stream.Push("topic", Message{
		UID: "1",
	})

	stream.Push("topic", Message{
		UID: "2",
	})

	time.Sleep(1 * time.Second)
	if atomic.LoadInt32(&count) != 2 {
		t.Fatal("fatal count message")
	}
}

func TestNotifyOneByOne(t *testing.T) {
	var count int32
	stream := NewMemoMessageStream()
	notifier := New(stream, MockPublisher{
		PublishFunc: func(ctx context.Context, messages []OutputMessage) error {
			atomic.AddInt32(&count, 1)
			return nil
		},
	}, Option{})
	defer notifier.Close()

	stream.Push("topic", Message{
		UID: "1",
	})

	time.Sleep(100 * time.Millisecond)
	if atomic.LoadInt32(&count) != 1 {
		t.Fatal("fatal count message")
	}

	stream.Push("topic", Message{
		UID: "2",
	})

	time.Sleep(100 * time.Millisecond)

	if atomic.LoadInt32(&count) != 2 {
		t.Fatal("fatal count message")
	}

	stream.Push("topic", Message{
		UID: "1",
	})

	time.Sleep(100 * time.Millisecond)

	if atomic.LoadInt32(&count) != 3 {
		t.Fatal("fatal count message")
	}
}