package workshop

import (
	"context"
	"runtime"
	"testing"
)

func BenchmarkNewPromise(b *testing.B) {
	pool := NewPool(runtime.GOMAXPROCS(0))

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var pc = Process{
			Process: func(ctx context.Context, last interface{}) (interface{}, error) {
				return nil, nil
			},
		}

		for pb.Next() {
			p := NewPromise(pool, pc)
			_, err := p.Get(context.Background())
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}