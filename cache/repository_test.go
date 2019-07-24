package cache

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestInterface(t *testing.T) {
	var a interface{} = ResourceKey{
		Category: "category",
	}
	var b interface{} = ResourceKey{
		Category: "category",
	}

	t.Log("a == b", a == b)
}

func BenchmarkStore(b *testing.B) {
	var c int32 = 0
	var store = &sync.Map{}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, ld := store.LoadOrStore(ResourceKey{
				Category: "ccccccc",
				ID: "",
			}, "c")
			if !ld {
				n := atomic.AddInt32(&c, 1)
				if n > 1 {
					b.Fatal("load count over 2")
				}
			}
		}
	})
}