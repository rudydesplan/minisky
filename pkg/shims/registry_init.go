package shims

import (
	"net/http"
	"strings"

	"minisky/pkg/registry"

	// Blank imports to trigger init() in all shim packages
	_ "minisky/pkg/shims/appengine"
	_ "minisky/pkg/shims/artifactregistry"
	_ "minisky/pkg/shims/bigquery"
	_ "minisky/pkg/shims/bigtable"
	_ "minisky/pkg/shims/cloudbuild"
	_ "minisky/pkg/shims/cloudkms"
	_ "minisky/pkg/shims/cloudsql"
	_ "minisky/pkg/shims/cloudtasks"
	_ "minisky/pkg/shims/compute"
	_ "minisky/pkg/shims/dataproc"
	_ "minisky/pkg/shims/dns"
	_ "minisky/pkg/shims/firebaseauth"
	_ "minisky/pkg/shims/firebasehosting"
	_ "minisky/pkg/shims/firebasertdb"
	_ "minisky/pkg/shims/gke"
	_ "minisky/pkg/shims/iam"
	_ "minisky/pkg/shims/iamcredentials"
	_ "minisky/pkg/shims/logging"
	_ "minisky/pkg/shims/memorystore"
	_ "minisky/pkg/shims/metadata"
	_ "minisky/pkg/shims/monitoring"
	_ "minisky/pkg/shims/pubsub"
	_ "minisky/pkg/shims/resourcemanager"
	_ "minisky/pkg/shims/scheduler"
	_ "minisky/pkg/shims/secretmanager"
	_ "minisky/pkg/shims/serverless"
	_ "minisky/pkg/shims/storage"
	_ "minisky/pkg/shims/sts"
	_ "minisky/pkg/shims/vertexai"
)

func init() {
	registry.RequireDocker(
		"firebasehosting.googleapis.com",
		"firebaseio.com",
		"identitytoolkit.googleapis.com",
		"pubsub.googleapis.com",
		"storage.googleapis.com",
	)
	registry.RequireDockerMutations(
		"appengine.googleapis.com",
		"artifactregistry.googleapis.com",
		"bigtable.googleapis.com",
		"bigtableadmin.googleapis.com",
		"cloudfunctions.googleapis.com",
		"dataproc.googleapis.com",
		"redis.googleapis.com",
		"run.googleapis.com",
		"sqladmin.googleapis.com",
	)
	registry.RequireDockerOperations("compute.googleapis.com", func(request *http.Request) bool {
		if request.Method == http.MethodGet || request.Method == http.MethodHead {
			return false
		}
		segments := strings.Split(strings.Trim(request.URL.Path, "/"), "/")
		contains := func(resource string) bool {
			for _, segment := range segments {
				if segment == resource {
					return true
				}
			}
			return false
		}
		switch {
		case contains("firewalls"):
			return request.Method == http.MethodPost || request.Method == http.MethodPut ||
				request.Method == http.MethodPatch || request.Method == http.MethodDelete
		case contains("subnetworks"):
			return request.Method == http.MethodPost || request.Method == http.MethodDelete
		case contains("networks"):
			return request.Method == http.MethodDelete
		case contains("instances"):
			return request.Method == http.MethodDelete ||
				(request.Method == http.MethodPost && segments[len(segments)-1] == "instances")
		}
		return false
	})
	registry.RequireDockerOperations("cloudbuild.googleapis.com", func(request *http.Request) bool {
		return request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/builds")
	})

	// Register services that don't have a custom Go shim but use direct Docker emulators
	registry.RegisterLazyDocker("firestore.googleapis.com")
	registry.RegisterLazyDocker("datastore.googleapis.com")
	registry.RegisterLazyDocker("spanner.googleapis.com")
}
