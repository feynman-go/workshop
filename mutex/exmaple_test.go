package mutex

import (
	"context"
	"time"
)

func ExampleBasicUse() {
	mx := &Mutex{}
	ctx, _ := context.WithTimeout(context.Background(), time.Second)
	if mx.Hold(ctx) { // other invoke will block in this method
		// do something
		mx.Release()
	}
}