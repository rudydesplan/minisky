package registry

import (
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
)

type sharedTestHandler struct{}

func (*sharedTestHandler) ServeHTTP(http.ResponseWriter, *http.Request) {}

func TestSharedHandlerCreatesOneOwnerConcurrently(t *testing.T) {
	ctx := &Context{shared: make(map[string]http.Handler)}
	var creates atomic.Int32
	factory := func() http.Handler {
		creates.Add(1)
		return &sharedTestHandler{}
	}
	handlers := make(chan http.Handler, 16)
	var group sync.WaitGroup
	for range 16 {
		group.Add(1)
		go func() {
			defer group.Done()
			handlers <- ctx.SharedHandler("bigtable", factory)
		}()
	}
	group.Wait()
	close(handlers)
	var first http.Handler
	for handler := range handlers {
		if first == nil {
			first = handler
		} else if first != handler {
			t.Fatal("shared domains received different owners")
		}
	}
	if creates.Load() != 1 {
		t.Fatalf("owner creations = %d, want 1", creates.Load())
	}
}

func TestRequireDockerRejectsHybridRegistration(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("hybrid Compute registration was accepted as pure Docker passthrough")
		}
	}()
	RequireDocker("compute.googleapis.com")
}
