package state

import (
	"bytes"
	"errors"
	"testing"
)

func TestPinnedLocalMarkerLifecycleIsGenerationConditional(t *testing.T) {
	store, err := New(t.TempDir(), "markers")
	if err != nil {
		t.Fatal(err)
	}
	const (
		namespace = ".cloudsql-local-runtime"
		name      = "generation"
	)
	first := []byte("first-generation\n")
	second := []byte("second-generation\n")
	if err := store.WriteLocalMarker(namespace, name, first); err != nil {
		t.Fatal(err)
	}
	payload, found, err := store.ReadLocalMarker(namespace, name)
	if err != nil || !found || !bytes.Equal(payload, first) {
		t.Fatalf("read marker found=%t payload=%q err=%v", found, payload, err)
	}
	if err := store.RemoveLocalMarker(namespace, name, second); !errors.Is(err, ErrMarkerMismatch) {
		t.Fatalf("mismatched removal error=%v, want ErrMarkerMismatch", err)
	}
	payload, found, err = store.ReadLocalMarker(namespace, name)
	if err != nil || !found || !bytes.Equal(payload, first) {
		t.Fatalf("mismatched removal changed marker found=%t payload=%q err=%v", found, payload, err)
	}
	if err := store.RemoveLocalMarker(namespace, name, first); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.ReadLocalMarker(namespace, name); err != nil || found {
		t.Fatalf("removed marker found=%t err=%v", found, err)
	}
}
