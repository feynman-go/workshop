package butler

import (
	"context"
	"log"
	"testing"
	"time"
)

func TestButler_GetServer(t *testing.T) {
	b := New()
	_, err := b.AddServer(Template{
		Config: ServerConfig{
			Name: "test-server",
		},
		Run: func(ctx RunContext) (int, string) {
			t := 0
			tc := time.NewTicker(time.Second)
			for {
				select {
				case <- tc.C:
					t++
					log.Println("ticker:", t)
				case <- ctx.Done():
					log.Println("return of context done")
					return 0, "return of context done"
				}
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	b.StartAll()

	ctx, _ := context.WithTimeout(context.Background(), 5 * time.Second)
	b.Until(ctx)
	time.Sleep(3 * time.Second)
}
