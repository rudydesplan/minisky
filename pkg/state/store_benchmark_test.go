package state

import (
	"fmt"
	"testing"
)

func BenchmarkStoreSave(b *testing.B) {
	store, err := New(b.TempDir(), "benchmark")
	if err != nil {
		b.Fatal(err)
	}
	payload := map[string]any{"name": "benchmark", "items": []int{1, 2, 3, 4}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := store.Save("benchmark/value", payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStoreLoad(b *testing.B) {
	store, err := New(b.TempDir(), "benchmark")
	if err != nil {
		b.Fatal(err)
	}
	if err := store.Save("benchmark/value", map[string]string{"value": "seed"}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var value map[string]string
		if err := store.Load("benchmark/value", &value); err != nil {
			b.Fatal(err)
		}
		if value["value"] != "seed" {
			b.Fatal(fmt.Errorf("unexpected value %q", value["value"]))
		}
	}
}
