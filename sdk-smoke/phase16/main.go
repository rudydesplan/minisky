package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	monitoring "google.golang.org/api/monitoring/v3"
	"google.golang.org/api/option"
)

const defaultSample = 42.125

type promQLResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			Value  []json.RawMessage `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "phase 16 Monitoring smoke failed: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	mode := strings.TrimSpace(os.Getenv("MINISKY_PHASE16_MODE"))
	if mode == "" {
		mode = "seed"
	}
	gateway := strings.TrimRight(strings.TrimSpace(os.Getenv("MINISKY_ENDPOINT")), "/")
	project := env("MINISKY_PROJECT_ID", "local-dev-project")
	metricType := strings.TrimSpace(os.Getenv("MINISKY_PHASE16_METRIC_TYPE"))
	if gateway == "" || metricType == "" {
		return fmt.Errorf("MINISKY_ENDPOINT and MINISKY_PHASE16_METRIC_TYPE are required")
	}
	sample, err := strconv.ParseFloat(env("MINISKY_PHASE16_SAMPLE", "42.125"), 64)
	if err != nil {
		return fmt.Errorf("parse sample: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	switch mode {
	case "seed":
		return seed(ctx, gateway, project, metricType, sample)
	case "query":
		return query(ctx, http.DefaultClient, gateway, project, metricType, sample)
	case "cleanup":
		return cleanup(ctx, gateway, project, metricType)
	default:
		return fmt.Errorf("unsupported MINISKY_PHASE16_MODE %q", mode)
	}
}

func monitoringService(ctx context.Context, gateway string) (*monitoring.Service, error) {
	return monitoring.NewService(ctx,
		option.WithoutAuthentication(),
		option.WithEndpoint(strings.TrimRight(gateway, "/")+"/_minisky/monitoring/"),
	)
}

func seed(ctx context.Context, gateway, project, metricType string, sample float64) error {
	service, err := monitoringService(ctx, gateway)
	if err != nil {
		return fmt.Errorf("create Monitoring client: %w", err)
	}
	descriptor := &monitoring.MetricDescriptor{
		Type: metricType, MetricKind: "GAUGE", ValueType: "DOUBLE",
		DisplayName: "MiniSky Phase 16 persisted sample",
	}
	if _, err := service.Projects.MetricDescriptors.Create("projects/"+project, descriptor).Context(ctx).Do(); err != nil {
		return fmt.Errorf("create metric descriptor: %w", err)
	}
	endTime := env("MINISKY_PHASE16_END_TIME", time.Now().UTC().Format(time.RFC3339Nano))
	request := &monitoring.CreateTimeSeriesRequest{TimeSeries: []*monitoring.TimeSeries{{
		Metric:   &monitoring.Metric{Type: metricType, Labels: map[string]string{"source": "phase16"}},
		Resource: &monitoring.MonitoredResource{Type: "global", Labels: map[string]string{"project_id": project}},
		Points: []*monitoring.Point{{
			Interval: &monitoring.TimeInterval{EndTime: endTime},
			Value:    &monitoring.TypedValue{DoubleValue: &sample},
		}},
	}}}
	if _, err := service.Projects.TimeSeries.Create("projects/"+project, request).Context(ctx).Do(); err != nil {
		return fmt.Errorf("write time series: %w", err)
	}
	fmt.Printf("seeded metric=%s sample=%s\n", metricType, strconv.FormatFloat(sample, 'g', -1, 64))
	return nil
}

func query(ctx context.Context, client *http.Client, gateway, project, metricType string, sample float64) error {
	endpoint := promQLURL(gateway, project, metricType)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("execute PromQL query: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("PromQL status=%d body=%s", response.StatusCode, body)
	}
	value, err := persistedSample(body, metricType)
	if err != nil {
		return err
	}
	expected := strconv.FormatFloat(sample, 'g', -1, 64)
	if value != expected {
		return fmt.Errorf("persisted sample=%q want=%q", value, expected)
	}
	fmt.Printf("persisted PromQL sample verified metric=%s value=%s\n", metricType, value)
	return nil
}

func cleanup(ctx context.Context, gateway, project, metricType string) error {
	service, err := monitoringService(ctx, gateway)
	if err != nil {
		return fmt.Errorf("create Monitoring client: %w", err)
	}
	name := "projects/" + project + "/metricDescriptors/" + metricType
	if _, err := service.Projects.MetricDescriptors.Delete(name).Context(ctx).Do(); err != nil {
		return fmt.Errorf("delete metric descriptor: %w", err)
	}
	fmt.Printf("deleted metric descriptor=%s\n", metricType)
	return nil
}

func promQLURL(gateway, project, metricType string) string {
	query := `{__name__="` + metricType + `"}`
	return strings.TrimRight(gateway, "/") + "/_minisky/monitoring/v1/projects/" +
		url.PathEscape(project) + "/location/global/prometheus/api/v1/query?query=" + url.QueryEscape(query)
}

func persistedSample(body []byte, metricType string) (string, error) {
	var response promQLResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return "", fmt.Errorf("decode PromQL response: %w", err)
	}
	if response.Status != "success" || response.Data.ResultType != "vector" || len(response.Data.Result) != 1 {
		return "", fmt.Errorf("unexpected PromQL response: %s", body)
	}
	result := response.Data.Result[0]
	if result.Metric["__name__"] != metricType || result.Metric["source"] != "phase16" || len(result.Value) != 2 {
		return "", fmt.Errorf("unexpected PromQL result: %s", body)
	}
	var value string
	if err := json.Unmarshal(result.Value[1], &value); err != nil {
		return "", fmt.Errorf("decode sample value: %w", err)
	}
	return value, nil
}

func env(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
