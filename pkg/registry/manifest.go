package registry

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"

	"minisky/pkg/orchestrator"
)

// UnsupportedContractPath is reserved for the executable unsupported-route
// contract. Registered HTTP routes intercept it before dispatching to a shim.
const UnsupportedContractPath = "/__minisky_contract__/unsupported"

type FidelityTier string

const (
	FidelityHigh        FidelityTier = "high"
	FidelityStandard    FidelityTier = "standard"
	FidelityPassthrough FidelityTier = "passthrough"
)

type SupportStatus string

const (
	SupportImplemented SupportStatus = "implemented"
	SupportDeferred    SupportStatus = "deferred"
)

type PersistenceCategory string

const (
	PersistenceMemory PersistenceCategory = "memory"
	PersistenceFile   PersistenceCategory = "file"
	PersistenceDocker PersistenceCategory = "docker"
	PersistenceHybrid PersistenceCategory = "hybrid"
	PersistenceStatic PersistenceCategory = "static"
)

// Service describes the support contract for an actually registered domain.
// Deferred domains have no fidelity tier because they expose only an explicit
// unsupported response.
// ProbeUnsupported is false only when constructing or invoking the service
// would require a lazy Docker backend.
type Service struct {
	Domain           string
	Support          SupportStatus
	Fidelity         FidelityTier
	Persistence      PersistenceCategory
	LazyDocker       bool
	ProbeUnsupported bool
	BackendContract  string
	DeferredReason   string
}

type serviceMetadata struct {
	fidelity         FidelityTier
	persistence      PersistenceCategory
	probeUnsupported bool
}

var serviceManifest = map[string]serviceMetadata{
	"aiplatform.googleapis.com":           {FidelityStandard, PersistenceFile, true},
	"appengine.googleapis.com":            {FidelityStandard, PersistenceHybrid, true},
	"artifactregistry.googleapis.com":     {FidelityStandard, PersistenceFile, true},
	"bigquery.googleapis.com":             {FidelityStandard, PersistenceFile, true},
	"bigtable.googleapis.com":             {FidelityStandard, PersistenceHybrid, true},
	"bigtableadmin.googleapis.com":        {FidelityStandard, PersistenceFile, true},
	"cloudbuild.googleapis.com":           {FidelityStandard, PersistenceHybrid, true},
	"cloudresourcemanager.googleapis.com": {FidelityStandard, PersistenceFile, true},
	"cloudfunctions.googleapis.com":       {FidelityStandard, PersistenceHybrid, true},
	"cloudkms.googleapis.com":             {FidelityStandard, PersistenceFile, true},
	"cloudscheduler.googleapis.com":       {FidelityStandard, PersistenceFile, true},
	"cloudtasks.googleapis.com":           {FidelityStandard, PersistenceFile, true},
	"compute.googleapis.com":              {FidelityStandard, PersistenceHybrid, true},
	"container.googleapis.com":            {FidelityStandard, PersistenceFile, true},
	"dataproc.googleapis.com":             {FidelityStandard, PersistenceHybrid, true},
	"datastore.googleapis.com":            {FidelityPassthrough, PersistenceDocker, false},
	"dns.googleapis.com":                  {FidelityStandard, PersistenceFile, true},
	"firebasehosting.googleapis.com":      {FidelityPassthrough, PersistenceDocker, true},
	"firebaseio.com":                      {FidelityPassthrough, PersistenceDocker, true},
	"firestore.googleapis.com":            {FidelityPassthrough, PersistenceDocker, false},
	"iam.googleapis.com":                  {FidelityStandard, PersistenceFile, true},
	"iamcredentials.googleapis.com":       {FidelityStandard, PersistenceStatic, true},
	"identitytoolkit.googleapis.com":      {FidelityPassthrough, PersistenceDocker, true},
	"logging.googleapis.com":              {FidelityStandard, PersistenceFile, true},
	"memcache.googleapis.com":             {"", PersistenceStatic, true},
	"metadata.google.internal":            {FidelityHigh, PersistenceStatic, true},
	"monitoring.googleapis.com":           {FidelityStandard, PersistenceFile, true},
	"pubsub.googleapis.com":               {FidelityPassthrough, PersistenceDocker, true},
	"redis.googleapis.com":                {FidelityStandard, PersistenceHybrid, true},
	"run.googleapis.com":                  {FidelityStandard, PersistenceHybrid, true},
	"secretmanager.googleapis.com":        {FidelityStandard, PersistenceFile, true},
	"spanner.googleapis.com":              {FidelityPassthrough, PersistenceDocker, false},
	"sqladmin.googleapis.com":             {FidelityStandard, PersistenceHybrid, true},
	"storage.googleapis.com":              {FidelityPassthrough, PersistenceDocker, true},
	"sts.googleapis.com":                  {FidelityStandard, PersistenceStatic, true},
}

var deferredServiceContracts = map[string]string{
	"memcache.googleapis.com": "Memorystore for Memcached is not implemented; every request returns 501 UNIMPLEMENTED",
}

// lazyBackendContracts records why these domains use backend-gated coverage
// instead of in-process CRUD probes. Their API behavior belongs to the named
// emulator and is executable only after a successful Docker cold start.
var lazyBackendContracts = map[string]string{
	"datastore.googleapis.com": "Google Cloud Datastore emulator; cold-start and backend errors are deterministic, CRUD requires Docker",
	"firestore.googleapis.com": "Google Cloud Firestore emulator; cold-start and backend errors are deterministic, CRUD requires Docker",
	"spanner.googleapis.com":   "Cloud Spanner emulator; cold-start and backend errors are deterministic, database behavior requires Docker",
}

// Services returns a stable manifest derived from current factory and lazy
// registrations. It fails if registration and metadata drift apart.
func Services() ([]Service, error) {
	registryMu.Lock()
	defer registryMu.Unlock()

	registered := make(map[string]bool, len(factories)+len(lazyDocker))
	for domain := range factories {
		registered[domain] = false
	}
	for domain := range lazyDocker {
		registered[domain] = true
	}

	var missing []string
	for domain := range registered {
		if _, ok := serviceManifest[domain]; !ok {
			missing = append(missing, domain)
		}
	}
	var stale []string
	for domain := range serviceManifest {
		if _, ok := registered[domain]; !ok {
			stale = append(stale, domain)
		}
	}
	if len(missing) > 0 || len(stale) > 0 {
		sort.Strings(missing)
		sort.Strings(stale)
		return nil, fmt.Errorf("service manifest drift: missing=%v stale=%v", missing, stale)
	}

	services := make([]Service, 0, len(registered))
	for domain, lazy := range registered {
		metadata := serviceManifest[domain]
		support := SupportImplemented
		deferredReason := deferredServiceContracts[domain]
		if deferredReason != "" {
			support = SupportDeferred
		}
		services = append(services, Service{
			Domain:           domain,
			Support:          support,
			Fidelity:         metadata.fidelity,
			Persistence:      metadata.persistence,
			LazyDocker:       lazy,
			ProbeUnsupported: metadata.probeUnsupported,
			BackendContract:  lazyBackendContracts[domain],
			DeferredReason:   deferredReason,
		})
	}
	sort.Slice(services, func(i, j int) bool {
		return services[i].Domain < services[j].Domain
	})
	return services, nil
}

// ContractHandlers constructs only handlers marked safe for an in-process
// contract probe. It deliberately omits PostBoot hooks and Docker services.
func ContractHandlers(opMgr *orchestrator.OperationManager, svcMgr *orchestrator.ServiceManager) (map[string]http.Handler, error) {
	services, err := Services()
	if err != nil {
		return nil, err
	}

	ctx := &Context{
		OpMgr:  opMgr,
		SvcMgr: svcMgr,
		shims:  make(map[string]http.Handler),
	}
	for _, service := range services {
		if !service.ProbeUnsupported {
			continue
		}
		factory := factories[service.Domain]
		if factory == nil {
			return nil, fmt.Errorf("service %s is probeable but has no factory", service.Domain)
		}
		handler := factory(ctx)
		ctx.shims[service.Domain] = handler
	}

	handlers := make(map[string]http.Handler, len(ctx.shims))
	for domain, handler := range ctx.shims {
		handlers[domain] = ContractHandler(domain, handler)
	}
	return handlers, nil
}

// ContractHandler preserves normal shim behavior while making the reserved
// unsupported-route probe deterministic at the service boundary.
func ContractHandler(domain string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != UnsupportedContractPath {
			next.ServeHTTP(w, r)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotImplemented)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{
				"code":    http.StatusNotImplemented,
				"message": "MiniSky: unsupported route for " + domain,
				"status":  "UNIMPLEMENTED",
			},
		})
	})
}
