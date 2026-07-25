package router

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"
	"time"

	"minisky/pkg/observability"
)

type QuotaRule struct {
	Limit  int
	Window time.Duration
}

type QuotaConfig struct {
	Default  *QuotaRule
	Services map[string]QuotaRule
	Projects map[string]QuotaRule
	Routes   map[string]QuotaRule
}

type QuotaDecision struct {
	Allowed    bool
	Scope      string
	RetryAfter time.Duration
}

type quotaBucket struct {
	count int
	reset time.Time
}

type QuotaLimiter struct {
	mu      sync.Mutex
	config  QuotaConfig
	buckets map[string]quotaBucket
	now     func() time.Time
}

func NewQuotaLimiter(config QuotaConfig, now func() time.Time) (*QuotaLimiter, error) {
	if now == nil {
		now = time.Now
	}
	if config.Default != nil {
		if err := validateQuotaRule(*config.Default); err != nil {
			return nil, fmt.Errorf("default quota: %w", err)
		}
	}
	for scope, rules := range map[string]map[string]QuotaRule{
		"service": config.Services,
		"project": config.Projects,
		"route":   config.Routes,
	} {
		for selector, rule := range rules {
			if strings.TrimSpace(selector) == "" {
				return nil, fmt.Errorf("%s quota selector is empty", scope)
			}
			if err := validateQuotaRule(rule); err != nil {
				return nil, fmt.Errorf("%s quota %q: %w", scope, selector, err)
			}
		}
	}
	return &QuotaLimiter{config: config, buckets: make(map[string]quotaBucket), now: now}, nil
}

func (q *QuotaLimiter) Allow(service, route, project string) QuotaDecision {
	if q == nil {
		return QuotaDecision{Allowed: true}
	}
	normalizedService := normalizeDomain(service)
	normalizedRoute := observability.NormalizeRoute(route)
	type matchedRule struct {
		scope    string
		selector string
		rule     QuotaRule
	}
	matched := make([]matchedRule, 0, 4)
	if rule, ok := q.config.Routes[normalizedService+" "+normalizedRoute]; ok {
		matched = append(matched, matchedRule{"route", normalizedService + " " + normalizedRoute, rule})
	} else if rule, ok := q.config.Routes[normalizedRoute]; ok {
		matched = append(matched, matchedRule{"route", normalizedRoute, rule})
	}
	if rule, ok := q.config.Services[normalizedService]; ok {
		matched = append(matched, matchedRule{"service", normalizedService, rule})
	}
	if project != "" {
		if rule, ok := q.config.Projects[project]; ok {
			matched = append(matched, matchedRule{"project", project, rule})
		}
	}
	if q.config.Default != nil {
		matched = append(matched, matchedRule{"default", "all", *q.config.Default})
	}
	if len(matched) == 0 {
		return QuotaDecision{Allowed: true}
	}

	now := q.now()
	q.mu.Lock()
	defer q.mu.Unlock()
	buckets := make([]quotaBucket, len(matched))
	for index, match := range matched {
		key := match.scope + ":" + match.selector
		bucket := q.buckets[key]
		if bucket.reset.IsZero() || !now.Before(bucket.reset) {
			bucket = quotaBucket{reset: now.Add(match.rule.Window)}
		}
		if bucket.count >= match.rule.Limit {
			return QuotaDecision{
				Allowed:    false,
				Scope:      match.scope,
				RetryAfter: bucket.reset.Sub(now),
			}
		}
		buckets[index] = bucket
	}
	for index, match := range matched {
		bucket := buckets[index]
		bucket.count++
		q.buckets[match.scope+":"+match.selector] = bucket
	}
	return QuotaDecision{Allowed: true}
}

type quotaRuleJSON struct {
	Limit  int    `json:"limit"`
	Window string `json:"window"`
}

type quotaConfigJSON struct {
	Default  *quotaRuleJSON           `json:"default,omitempty"`
	Services map[string]quotaRuleJSON `json:"services,omitempty"`
	Projects map[string]quotaRuleJSON `json:"projects,omitempty"`
	Routes   map[string]quotaRuleJSON `json:"routes,omitempty"`
}

func ParseQuotaConfigJSON(payload string, now func() time.Time) (*QuotaLimiter, error) {
	if strings.TrimSpace(payload) == "" {
		return nil, nil
	}
	var wire quotaConfigJSON
	decoder := json.NewDecoder(strings.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return nil, fmt.Errorf("decode quota configuration: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, errors.New("quota configuration contains trailing JSON")
	}
	var config QuotaConfig
	var err error
	if wire.Default != nil {
		rule, parseErr := parseQuotaRule(*wire.Default)
		if parseErr != nil {
			return nil, fmt.Errorf("default quota: %w", parseErr)
		}
		config.Default = &rule
	}
	if config.Services, err = parseQuotaRules(wire.Services); err != nil {
		return nil, fmt.Errorf("service quotas: %w", err)
	}
	if config.Projects, err = parseQuotaRules(wire.Projects); err != nil {
		return nil, fmt.Errorf("project quotas: %w", err)
	}
	if config.Routes, err = parseQuotaRules(wire.Routes); err != nil {
		return nil, fmt.Errorf("route quotas: %w", err)
	}
	return NewQuotaLimiter(config, now)
}

func parseQuotaRules(values map[string]quotaRuleJSON) (map[string]QuotaRule, error) {
	if values == nil {
		return nil, nil
	}
	result := make(map[string]QuotaRule, len(values))
	for selector, value := range values {
		rule, err := parseQuotaRule(value)
		if err != nil {
			return nil, fmt.Errorf("%q: %w", selector, err)
		}
		result[selector] = rule
	}
	return result, nil
}

func parseQuotaRule(value quotaRuleJSON) (QuotaRule, error) {
	window, err := time.ParseDuration(value.Window)
	if err != nil {
		return QuotaRule{}, errors.New("window must be a Go duration such as 1s or 1m")
	}
	rule := QuotaRule{Limit: value.Limit, Window: window}
	return rule, validateQuotaRule(rule)
}

func validateQuotaRule(rule QuotaRule) error {
	if rule.Limit <= 0 {
		return errors.New("limit must be positive")
	}
	if rule.Window < time.Millisecond || rule.Window > 24*time.Hour {
		return errors.New("window must be between 1ms and 24h")
	}
	return nil
}

func quotaRetryAfterSeconds(duration time.Duration) int {
	if duration <= 0 {
		return 1
	}
	return max(1, int(math.Ceil(duration.Seconds())))
}
