package pagination

import (
	"errors"
	"testing"
)

func TestPageTokensAreOpaqueDeterministicAndScopeBound(t *testing.T) {
	t.Parallel()

	items := []string{"a", "b", "c"}
	scope := Scope{
		Service: "workflows.googleapis.com",
		Parent:  "projects/demo/locations/us",
		Filter:  "state=ACTIVE",
		OrderBy: "name",
	}
	first, token, err := Page(items, 2, "", scope, func(item string) string { return item })
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 || token == "" || token == "b" {
		t.Fatalf("first page = %v token=%q", first, token)
	}
	_, repeat, err := Page(items, 2, "", scope, func(item string) string { return item })
	if err != nil || repeat != token {
		t.Fatalf("deterministic token = %q, %v; want %q", repeat, err, token)
	}
	second, next, err := Page(items, 2, token, scope, func(item string) string { return item })
	if err != nil || len(second) != 1 || second[0] != "c" || next != "" {
		t.Fatalf("second page = %v token=%q err=%v", second, next, err)
	}

	for name, invalidScope := range map[string]Scope{
		"service": {Service: "batch.googleapis.com", Parent: scope.Parent, Filter: scope.Filter, OrderBy: scope.OrderBy},
		"parent":  {Service: scope.Service, Parent: "projects/other/locations/us", Filter: scope.Filter, OrderBy: scope.OrderBy},
		"filter":  {Service: scope.Service, Parent: scope.Parent, Filter: "state=FAILED", OrderBy: scope.OrderBy},
		"order":   {Service: scope.Service, Parent: scope.Parent, Filter: scope.Filter, OrderBy: "createTime"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := Page(items, 2, token, invalidScope, func(item string) string { return item }); !errors.Is(err, ErrInvalidToken) {
				t.Fatalf("Page() error = %v, want ErrInvalidToken", err)
			}
		})
	}
}

func TestPageRejectsMalformedAndStaleTokens(t *testing.T) {
	t.Parallel()

	scope := Scope{Service: "batch.googleapis.com", Parent: "projects/demo/locations/us"}
	key := func(item string) string { return item }
	items := []string{"a", "b", "c"}
	_, token, err := Page(items, 1, "", scope, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := Page(items, 1, "not-a-token", scope, key); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("malformed token error = %v", err)
	}
	if _, _, err := Page([]string{"a", "changed", "c"}, 1, token, scope, key); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("stale token error = %v", err)
	}
}

func TestPageReturnsNonNilEmptySlice(t *testing.T) {
	page, token, err := Page([]string{}, 10, "", Scope{Service: "test", Parent: "parent"},
		func(item string) string { return item })
	if err != nil {
		t.Fatal(err)
	}
	if page == nil || len(page) != 0 || token != "" {
		t.Fatalf("page=%v token=%q, want non-nil empty page", page, token)
	}
}
