package metadata

import (
	"io"
	"log"
	"net/http"
	"testing"
)

type discardResponseWriter struct {
	header http.Header
	status int
}

func (w *discardResponseWriter) Header() http.Header { return w.header }
func (w *discardResponseWriter) Write(payload []byte) (int, error) {
	return len(payload), nil
}
func (w *discardResponseWriter) WriteHeader(status int) { w.status = status }

func BenchmarkProjectMetadataLookup(b *testing.B) {
	previous := log.Writer()
	log.SetOutput(io.Discard)
	b.Cleanup(func() { log.SetOutput(previous) })

	api := &API{meta: defaultMeta}
	request, err := http.NewRequest(http.MethodGet, "http://metadata.google.internal/computeMetadata/v1/project/project-id", nil)
	if err != nil {
		b.Fatal(err)
	}
	request.Header.Set("Metadata-Flavor", "Google")
	response := &discardResponseWriter{header: make(http.Header)}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		response.status = 0
		api.ServeHTTP(response, request)
		if response.status != http.StatusOK {
			b.Fatalf("status = %d", response.status)
		}
	}
}
