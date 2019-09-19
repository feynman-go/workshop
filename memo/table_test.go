package memo

import (
	"testing"
	"time"
)

func TestTableSetGet(t *testing.T) {
	tb := NewTable(0, 0, nil)
	tb.Set(10, "10")
	v, ok := tb.Get(10)
	if !ok {
		t.Fatal("get not ok")
	}
	if v != "10" {
		t.Fatal("get value not equal")
	}
}

func TestTableTimeOut(t *testing.T) {
	tb := NewTable(0, time.Second, nil)
	tb.Set(10, "10")
	v, ok := tb.Get(10)
	if !ok {
		t.Fatal("get not ok")
	}
	if v != "10" {
		t.Fatal("get value not equal")
	}

	time.Sleep(time.Second + 100 * time.Millisecond)
	_, ok = tb.Get(10)
	if ok {
		t.Fatal("get should return empty")
	}
}

func TestTableFetcher(t *testing.T) {
	tb := NewTable(0, time.Second, nil)
	tb.Set(10, "10")
	v, ok := tb.Get(10)
	if !ok {
		t.Fatal("get not ok")
	}

	v, loaded := tb.Fetch(10, "11")
	if !loaded {
		t.Fatal("fetch not loaded")
	}

	if v != "10" {
		t.Fatal("get value not equal")
	}

	v, loaded = tb.Fetch(20, "20")
	if loaded {
		t.Fatal("fetch is loaded")
	}

	if v != "20" {
		t.Fatal("get value not equal")
	}
}

func TestTableOverMaxEntry(t *testing.T) {
	tb := NewTable(2, time.Second, nil)
	tb.Set(1, "10")
	tb.Set(2, "20")
	v, ok := tb.Get(2)
	if !ok {
		t.Fatal("get not ok")
	}
	if v != "20" {
		t.Fatal("get value not equal")
	}
	tb.Set(3, "30")
	_, ok = tb.Get(1)
	if ok {
		t.Fatal("get should return empty")
	}
}
