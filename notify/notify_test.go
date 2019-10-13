package notify

import (
	"context"
	"testing"
	"time"
)

func TestNotifySend(t *testing.T) {
	stream := NewMemoMessageStream()
	iterator := NewIterator(stream, Option{})
	defer func() {
		iterator.CloseWithContext(context.Background())
	}()

	stream.Push(Notification{})

	iterator.Next(context.Background())
	if iterator.err != nil {
		t.Fatal(iterator.err)
	}

	time.Sleep(100 * time.Millisecond)
	if len(iterator.Batch()) != 1 {
		t.Fatal("fatal count message")
	}
}

func TestNotifySendMulti(t *testing.T) {
	stream := NewMemoMessageStream()
	iterator := NewIterator(stream, Option{})
	defer func() {
		iterator.CloseWithContext(context.Background())
	}()

	stream.Push(Notification{})
	iterator.Next(context.Background())
	if iterator.Err() != nil {
		t.Fatal(iterator.Err())
	}
	if len(iterator.Batch()) != 1 {
		t.Fatal("fatal count message")
	}

	stream.Push(Notification{})
	iterator.Next(context.Background())
	if iterator.Err() != nil {
		t.Fatal(iterator.Err())
	}
	if len(iterator.Batch()) != 1 {
		t.Fatal("fatal count message")
	}



}

func TestNotifyOneByOne(t *testing.T) {
	stream := NewMemoMessageStream()
	iterator := NewIterator(stream, Option{})
	defer func() {
		iterator.CloseWithContext(context.Background())
	}()

	stream.Push(Notification{})

	iterator.Next(context.Background())
	if iterator.Err() != nil {
		t.Fatal(iterator.Err())
	}
	if len(iterator.Batch()) != 1 {
		t.Fatal("fatal count message")
	}

	stream.Push(Notification{})
	iterator.Next(context.Background())
	if iterator.Err() != nil {
		t.Fatal(iterator.Err())
	}

	if len(iterator.Batch()) != 1 {
		t.Fatal("fatal count message")
	}

	stream.Push(Notification{})

	iterator.Next(context.Background())
	if iterator.Err() != nil {
		t.Fatal(iterator.Err())
	}

	if len(iterator.Batch()) != 1 {
		t.Fatal("fatal count message")
	}

	stream.Push(Notification{})

	time.Sleep(100 * time.Millisecond)
	iterator.Next(context.Background())
	if iterator.Err() != nil {
		t.Fatal(iterator.Err())
	}

	if len(iterator.Batch()) != 1 {
		t.Fatal("fatal count message")
	}
}