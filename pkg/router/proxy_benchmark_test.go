package router

import (
	"io"
	"log"
	"net/http"
	"testing"
)

type benchmarkResponseWriter struct {
	header http.Header
	status int
}

func (w *benchmarkResponseWriter) Header() http.Header { return w.header }
func (w *benchmarkResponseWriter) Write(payload []byte) (int, error) {
	return len(payload), nil
}
func (w *benchmarkResponseWriter) WriteHeader(status int) { w.status = status }

func BenchmarkGatewayRouting(b *testing.B) {
	previous := log.Writer()
	log.SetOutput(io.Discard)
	b.Cleanup(func() { log.SetOutput(previous) })

	proxy := NewProxyRouterWithManager(nil)
	proxy.RegisterShim("compute.googleapis.com", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	request, err := http.NewRequest(http.MethodGet, "https://compute.googleapis.com/compute/v1/projects/demo/zones/us-central1-a/instances", nil)
	if err != nil {
		b.Fatal(err)
	}
	response := &benchmarkResponseWriter{header: make(http.Header)}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		response.status = 0
		proxy.ServeHTTP(response, request)
		if response.status != http.StatusNoContent {
			b.Fatalf("status = %d", response.status)
		}
	}
}
