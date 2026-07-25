package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	maxBenchmarkDuration    = 60 * time.Second
	maxBenchmarkConcurrency = 128
	maxLatencySamples       = 1_000_000
	maxErrorSamples         = 20
	maxResponseBytes        = 1 << 20
)

type benchmarkResult struct {
	StartedAt       time.Time     `json:"startedAt"`
	Duration        time.Duration `json:"duration"`
	Concurrency     int           `json:"concurrency"`
	Method          string        `json:"method"`
	Target          string        `json:"target"`
	Profile         string        `json:"profile"`
	OS              string        `json:"os"`
	Architecture    string        `json:"architecture"`
	GoVersion       string        `json:"goVersion"`
	CPUs            int           `json:"cpus"`
	Requests        uint64        `json:"requests"`
	RequestsPerSec  float64       `json:"requestsPerSecond"`
	TransportErrors uint64        `json:"transportErrors"`
	Non2xxResponses uint64        `json:"non2xxResponses"`
	ResponseBytes   uint64        `json:"responseBytes"`
	LatencySamples  int           `json:"latencySamples"`
	P50             time.Duration `json:"p50"`
	P95             time.Duration `json:"p95"`
	P99             time.Duration `json:"p99"`
	Errors          []string      `json:"errors,omitempty"`
	Command         []string      `json:"command"`
}

type benchmarkCollector struct {
	requests        atomic.Uint64
	transportErrors atomic.Uint64
	non2xx          atomic.Uint64
	bytes           atomic.Uint64
	mu              sync.Mutex
	latencies       []time.Duration
	errors          []string
}

func main() {
	endpoint := flag.String("endpoint", "http://127.0.0.1:8080", "local MiniSky endpoint")
	path := flag.String("path", "/healthz", "request path")
	method := flag.String("method", http.MethodGet, "HTTP method")
	duration := flag.Duration("duration", 5*time.Second, "bounded measurement duration")
	concurrency := flag.Int("concurrency", 4, "bounded worker count")
	timeout := flag.Duration("timeout", 2*time.Second, "per-request timeout")
	output := flag.String("output", "", "optional JSON output path")
	flag.Parse()

	if *duration <= 0 || *duration > maxBenchmarkDuration {
		exitf("duration must be between 1ns and %s", maxBenchmarkDuration)
	}
	if *concurrency < 1 || *concurrency > maxBenchmarkConcurrency {
		exitf("concurrency must be between 1 and %d", maxBenchmarkConcurrency)
	}
	if *timeout <= 0 || *timeout > 30*time.Second {
		exitf("timeout must be between 1ns and 30s")
	}
	methodValue := strings.ToUpper(strings.TrimSpace(*method))
	if !allowedBenchmarkMethod(methodValue) {
		exitf("method %q is not supported", methodValue)
	}
	base, err := url.Parse(*endpoint)
	if err != nil {
		exitf("invalid endpoint: %v", err)
	}
	relative, err := url.Parse(*path)
	if err != nil {
		exitf("invalid path: %v", err)
	}
	target := base.ResolveReference(relative).String()
	if err := validateBenchmarkTarget(target); err != nil {
		exitf("%v", err)
	}

	result := runBenchmark(target, methodValue, *duration, *timeout, *concurrency)
	payload, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		exitf("encode result: %v", err)
	}
	payload = append(payload, '\n')
	if *output == "" {
		_, _ = os.Stdout.Write(payload)
	} else if err := os.WriteFile(*output, payload, 0o600); err != nil {
		exitf("write result: %v", err)
	}
	if result.TransportErrors > 0 || result.Non2xxResponses > 0 {
		os.Exit(1)
	}
}

func runBenchmark(target, method string, duration, timeout time.Duration, concurrency int) benchmarkResult {
	started := time.Now().UTC()
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	client := &http.Client{Timeout: timeout}
	collector := &benchmarkCollector{latencies: make([]time.Duration, 0, 4096)}
	var workers sync.WaitGroup
	for worker := 0; worker < concurrency; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for ctx.Err() == nil {
				request, err := http.NewRequestWithContext(ctx, method, target, nil)
				if err != nil {
					collector.addError(err)
					return
				}
				requestStarted := time.Now()
				response, err := client.Do(request)
				latency := time.Since(requestStarted)
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) && ctx.Err() != nil {
					return
				}
				collector.requests.Add(1)
				collector.addLatency(latency)
				if err != nil {
					collector.transportErrors.Add(1)
					collector.addError(err)
					continue
				}
				read, readErr := io.Copy(io.Discard, io.LimitReader(response.Body, maxResponseBytes+1))
				_ = response.Body.Close()
				collector.bytes.Add(uint64(read))
				if readErr != nil {
					collector.transportErrors.Add(1)
					collector.addError(readErr)
				}
				if read > maxResponseBytes {
					collector.transportErrors.Add(1)
					collector.addError(errors.New("response exceeded 1 MiB benchmark limit"))
				}
				if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
					collector.non2xx.Add(1)
					collector.addError(fmt.Errorf("HTTP %d", response.StatusCode))
				}
			}
		}()
	}
	workers.Wait()
	elapsed := time.Since(started)
	collector.mu.Lock()
	latencies := append([]time.Duration(nil), collector.latencies...)
	errorSamples := append([]string(nil), collector.errors...)
	collector.mu.Unlock()
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	requests := collector.requests.Load()
	return benchmarkResult{
		StartedAt:       started,
		Duration:        elapsed,
		Concurrency:     concurrency,
		Method:          method,
		Target:          target,
		Profile:         environmentOr("MINISKY_PROFILE", "default"),
		OS:              runtime.GOOS,
		Architecture:    runtime.GOARCH,
		GoVersion:       runtime.Version(),
		CPUs:            runtime.NumCPU(),
		Requests:        requests,
		RequestsPerSec:  float64(requests) / elapsed.Seconds(),
		TransportErrors: collector.transportErrors.Load(),
		Non2xxResponses: collector.non2xx.Load(),
		ResponseBytes:   collector.bytes.Load(),
		LatencySamples:  len(latencies),
		P50:             percentile(latencies, 0.50),
		P95:             percentile(latencies, 0.95),
		P99:             percentile(latencies, 0.99),
		Errors:          errorSamples,
		Command:         append([]string(nil), os.Args...),
	}
}

func (c *benchmarkCollector) addLatency(value time.Duration) {
	c.mu.Lock()
	if len(c.latencies) < maxLatencySamples {
		c.latencies = append(c.latencies, value)
	}
	c.mu.Unlock()
}

func (c *benchmarkCollector) addError(err error) {
	c.mu.Lock()
	if len(c.errors) < maxErrorSamples {
		c.errors = append(c.errors, err.Error())
	}
	c.mu.Unlock()
}

func validateBenchmarkTarget(target string) error {
	parsed, err := url.Parse(target)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return errors.New("benchmark target must be an absolute HTTP URL")
	}
	host := parsed.Hostname()
	if host == "localhost" {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return errors.New("benchmark target must resolve explicitly to localhost or a loopback IP")
	}
	return nil
}

func percentile(sorted []time.Duration, quantile float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	index := int(float64(len(sorted)-1)*quantile + 0.5)
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

func allowedBenchmarkMethod(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	default:
		return false
	}
}

func environmentOr(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func exitf(format string, args ...any) {
	_, _ = fmt.Fprintf(os.Stderr, "api-benchmark: "+format+"\n", args...)
	os.Exit(2)
}
