package main

import (
	"testing"
	"time"
)

func TestValidateBenchmarkTargetAllowsOnlyLoopback(t *testing.T) {
	if err := validateBenchmarkTarget("http://127.0.0.1:8080/healthz"); err != nil {
		t.Fatal(err)
	}
	for _, target := range []string{
		"https://example.com/",
		"http://0.0.0.0:8080/",
		"file:///tmp/socket",
	} {
		if err := validateBenchmarkTarget(target); err == nil {
			t.Fatalf("target %q was accepted", target)
		}
	}
}

func TestPercentileUsesSortedBoundedSample(t *testing.T) {
	values := []time.Duration{time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond, 4 * time.Millisecond}
	if got := percentile(values, 0.95); got != 4*time.Millisecond {
		t.Fatalf("p95 = %s", got)
	}
}
