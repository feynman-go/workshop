### Workshop
     Workshop is a latency and fault tolerance library designed to isolate points of access to remote systems, services and 3rd party libraries, stop cascading failure and enable resilience in complex distributed systems where failure is inevitable.
     
     + Inspire by [Netflix/Hystrix](https://github.com/Netflix/Hystrix)
     
     + Promise like interface
     
     + support intercepter to extand like rate / limit
     
##### examples
 
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