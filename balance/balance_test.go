package balance

import (
	"testing"
)

func TestBalance(t *testing.T) {
	b := New(&RoundRobin{}, []*Instance{
		NewInstance(0, 0),
		NewInstance(1, 1),
		NewInstance(2, 2),
	})

	for i := 0; i < 100; i++ {
		instance := b.Pick(0)
		switch (i + 1) % 6 {
		case 0:
			if v := instance.Value(); v != 0 {
				t.Fatalf("expect instance value %v but %v", 0, v)
			}
		case 1, 2:
			if v := instance.Value(); v != 1 {
				t.Fatalf("expect instance value %v but %v", 1, v)
			}
		case 3, 4, 5:
			if v := instance.Value(); v != 2 {
				t.Fatalf("expect instance value %v but %v", 2, v)
			}
		}
	}
}
