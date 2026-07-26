package dashboard

import (
	"fmt"
	"net/http"
	"strings"
)

type dashboardGKERoute struct {
	project    string
	zone       string
	cluster    string
	kubeconfig bool
}

func parseDashboardGKERoute(r *http.Request) (dashboardGKERoute, error) {
	path := r.URL.Path
	escaped := strings.ToLower(r.URL.EscapedPath())
	if strings.Contains(escaped, "%2f") || strings.Contains(escaped, "%5c") ||
		strings.Contains(path, "//") {
		return dashboardGKERoute{}, fmt.Errorf("invalid GKE cluster route")
	}
	if strings.HasSuffix(path, "/") {
		path = strings.TrimSuffix(path, "/")
	}
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if len(parts) < 8 || len(parts) > 10 ||
		strings.Join(parts[:4], "/") != "api/manage/gke/projects" ||
		parts[4] == "" || parts[5] != "zones" || parts[6] == "" ||
		parts[7] != "clusters" {
		return dashboardGKERoute{}, fmt.Errorf("invalid GKE cluster route")
	}
	route := dashboardGKERoute{project: parts[4], zone: parts[6]}
	if len(parts) >= 9 {
		if parts[8] == "" {
			return dashboardGKERoute{}, fmt.Errorf("invalid GKE cluster route")
		}
		route.cluster = parts[8]
	}
	if len(parts) == 10 {
		if route.cluster == "" || parts[9] != "config" {
			return dashboardGKERoute{}, fmt.Errorf("invalid GKE cluster route")
		}
		route.kubeconfig = true
	}
	return route, nil
}

func isDashboardGKERoute(r *http.Request) bool {
	return strings.HasPrefix(r.URL.Path, "/api/manage/gke/")
}
