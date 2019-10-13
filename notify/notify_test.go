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
		iterator.Close(context.Background())
	}()

	stream.Push(Notification{})

	it, err := iterator.CommitAndWaitNext(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)
	if len(it.Batch()) != 1 {
		t.Fatal("fatal count message")
	}
}

func TestNotifySendMulti(t *testing.T) {
	stream := NewMemoMessageStream()
	iterator := NewIterator(stream, Option{})
	defer func() {
		iterator.Close(context.Background())
	}()

	var err error

	stream.Push(Notification{})
	iterator, err = iterator.CommitAndWaitNext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(iterator.Batch()) != 1 {
		t.Fatal("fatal count message")
	}

	stream.Push(Notification{})
	iterator, err = iterator.CommitAndWaitNext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(iterator.Batch()) != 1 {
		t.Fatal("fatal count message")
	}



}

func TestNotifyOneByOne(t *testing.T) {
	stream := NewMemoMessageStream()
	iterator := NewIterator(stream, Option{})
	defer func() {
		iterator.Close(context.Background())
	}()

	var err error
	stream.Push(Notification{})

	iterator, err = iterator.CommitAndWaitNext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(iterator.Batch()) != 1 {
		t.Fatal("fatal count message")
	}

	stream.Push(Notification{})
	iterator, err = iterator.CommitAndWaitNext(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if len(iterator.Batch()) != 1 {
		t.Fatal("fatal count message")
	}

	stream.Push(Notification{})

	iterator, err = iterator.CommitAndWaitNext(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if len(iterator.Batch()) != 1 {
		t.Fatal("fatal count message")
	}

	stream.Push(Notification{})

	time.Sleep(100 * time.Millisecond)
	iterator, err = iterator.CommitAndWaitNext(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if len(iterator.Batch()) != 1 {
		t.Fatal("fatal count message")
	}
}