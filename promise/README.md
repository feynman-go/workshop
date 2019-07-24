### Workshop
     Workshop is a latency and fault tolerance library designed to isolate points of access to remote systems, services and 3rd party libraries, stop cascading failure and enable resilience in complex distributed systems where failure is inevitable.
     
     + Inspire by [Netflix/Hystrix](https://github.com/Netflix/Hystrix)
     
     + Promise like interface
     
     + support intercepter to extand like rate / limit
     
##### examples
 
 + promise
```
    func ExamplePromise() {
    	pool := NewPool(3)
    	pms := NewPromise(pool, Process{
    		Process: func(ctx context.Context, last interface{}) (interface{}, error) {
    			// .... Do task
    			return true, nil
    		},
    	})
    
    	// close promise will close task context
    	defer pms.Close()
    
    	ctx, _ := context.WithTimeout(context.Background(), 100 * time.Millisecond)
    	_, err := pms.Get(ctx)
    	if err != nil {
    		// ... Handle err
    	}
    }

```

  + promise chain
```
func ExamplePromiseChain() {
   	pool := NewPool(3)
   	pms := NewPromise(pool, Process{
   		Process: func(ctx context.Context, last interface{}) (interface{}, error) {
   			// .... Do task 1
   			log.Println("do task one")
   			return true, nil
   		},
   	}).Then(Process{
   		Process: func(ctx context.Context, last interface{}) (interface{}, error) {
   			// .... Do task 2
   			log.Println("do task two")
   			return true, nil
   		},
   	})
   
   	// close promise will close task context
   	defer pms.Close()
   
   	ctx, _ := context.WithTimeout(context.Background(), 100 * time.Millisecond)
   	_, err := pms.Get(ctx)
   	if err != nil {
   		// ... Handle err
   	}
}
```

 + retry
```
func ExamplePromiseRetry() {
	pool := NewPool(3)
	pms := NewPromise(pool, Process{
		Process: func(ctx context.Context, last interface{}) (interface{}, error) {
			// .... Do task 1
			log.Println("do task one")
			return true, nil
		},
	}).RecoverAndRetry(ExceptionProcess{
		Process: func(ctx context.Context, err error, last interface{}) (interface{}, error) {
			// .... try recover
			log.Println("try recover")
			return last, nil
		},
	})

	// close promise will close task context
	defer pms.Close()

	ctx, _ := context.WithTimeout(context.Background(), 100 * time.Millisecond)
	_, err := pms.Get(ctx)
	if err != nil {
		// ... Handle err
	}
}
```

+ breaker
```
func ExampleBreaker() {
	var failedLimit float64 = 6
	bucketSize := 6
	middle := NewBreakerMiddle(time.Second, failedLimit, bucketSize)

	pool := workshop.NewPool(4)
	var err error
	pms := workshop.NewPromise(pool, workshop.Process{
		Process: func(ctx context.Context, last interface{}) (interface{}, error) {
			return nil, nil
		},
		EventKey: 1,
		Middles: []workshop.Middle{middle}, // use breaker middle
	})
	err = pms.Wait(context.Background())
	if err == BreakerOpenErr {
		// rate limit
		log.Println("breaker open")
	}
}
```

+ rate limit
```
func ExampleRate() {
	pool := workshop.NewPool(4)
	var md = NewRateMiddle(6, 6)
	var err error
	pms := workshop.NewPromise(pool, workshop.Process{
		Process: func(ctx context.Context, last interface{}) (interface{}, error) {
			return nil, nil
		},
		EventKey: 1,
		Middles: []workshop.Middle{md}, // use rate middle
	})
	err = pms.Wait(context.Background())
	if err == RateLimitErr {
		// rate limit
		log.Println("rate limit occur")
	}
}

```