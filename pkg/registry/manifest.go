package registry

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"

	"minisky/pkg/orchestrator"
)

// UnsupportedContractPath is reserved for the executable unsupported-route
// contract. Registered HTTP routes intercept it before dispatching to a shim.
const UnsupportedContractPath = "/__minisky_contract__/unsupported"

// ExperimentalServicesEnv explicitly enables handlers whose promotion evidence
// remains incomplete. The only enabling value is "1".
const ExperimentalServicesEnv = "MINISKY_ENABLE_EXPERIMENTAL_SERVICES"

type FidelityTier string

const (
	FidelityHigh        FidelityTier = "high"
	FidelityStandard    FidelityTier = "standard"
	FidelityPassthrough FidelityTier = "passthrough"
)

type SupportStatus string

const (
	SupportImplemented  SupportStatus = "implemented"
	SupportDeferred     SupportStatus = "deferred"
	SupportExperimental SupportStatus = "experimental"
)

type PersistenceCategory string

const (
	PersistenceMemory PersistenceCategory = "memory"
	PersistenceFile   PersistenceCategory = "file"
	PersistenceDocker PersistenceCategory = "docker"
	PersistenceHybrid PersistenceCategory = "hybrid"
	PersistenceStatic PersistenceCategory = "static"
)

type DockerRequestBody string

const (
	DockerRequestBodyBatchRunnable  DockerRequestBody = "batch-runnable"
	DockerRequestBodyAlloyDBPrimary DockerRequestBody = "alloydb-primary"
)

// DockerOperation identifies one executable HTTP mutation that must fail
// before handler dispatch when the current boot has no Docker backend.
type DockerOperation struct {
	HTTPMethod  string
	PathGlob    string
	RequestBody DockerRequestBody
}

// Service describes the support contract for an actually registered domain.
// Deferred and experimental domains have no fidelity tier because they are not
// promoted implementation claims.
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
	DockerOperations []DockerOperation
	SupportReason    string
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
	"cloudfunctions.googleapis.com":       {FidelityStandard, PersistenceHybrid, true},
	"cloudkms.googleapis.com":             {FidelityStandard, PersistenceFile, true},
	"cloudprofiler.googleapis.com":        {"", PersistenceMemory, true},
	"cloudresourcemanager.googleapis.com": {FidelityStandard, PersistenceFile, true},
	"cloudscheduler.googleapis.com":       {FidelityStandard, PersistenceFile, true},
	"cloudtasks.googleapis.com":           {FidelityStandard, PersistenceFile, true},
	"compute.googleapis.com":              {FidelityStandard, PersistenceHybrid, true},
	"container.googleapis.com":            {FidelityStandard, PersistenceFile, true},
	"dataproc.googleapis.com":             {FidelityStandard, PersistenceHybrid, true},
	"datastore.googleapis.com":            {FidelityPassthrough, PersistenceDocker, false},
	"dns.googleapis.com":                  {FidelityStandard, PersistenceFile, true},
	"documentai.googleapis.com":           {"", PersistenceFile, true},
	"eventarc.googleapis.com":             {"", PersistenceFile, true},
	"workflows.googleapis.com":            {"", PersistenceFile, true},
	"workflowexecutions.googleapis.com":   {"", PersistenceFile, true},
	"batch.googleapis.com":                {"", PersistenceHybrid, true},
	"binaryauthorization.googleapis.com":  {"", PersistenceFile, true},
	"dataflow.googleapis.com":             {"", PersistenceFile, true},
	"alloydb.googleapis.com":              {"", PersistenceHybrid, true},
	"apigateway.googleapis.com":           {"", PersistenceFile, true},
	"clouddeploy.googleapis.com":          {"", PersistenceFile, true},
	"composer.googleapis.com":             {"", PersistenceHybrid, true},
	"dataform.googleapis.com":             {"", PersistenceFile, true},
	"file.googleapis.com":                 {"", PersistenceFile, true},
	"managedkafka.googleapis.com":         {"", PersistenceHybrid, true},
	"networksecurity.googleapis.com":      {"", PersistenceFile, true},
	"networkservices.googleapis.com":      {"", PersistenceFile, true},
	"orgpolicy.googleapis.com":            {"", PersistenceFile, true},
	"servicedirectory.googleapis.com":     {"", PersistenceFile, true},
	"dialogflow.googleapis.com":           {"", PersistenceFile, true},
	"language.googleapis.com":             {"", PersistenceStatic, true},
	"privateca.googleapis.com":            {"", PersistenceFile, true},
	"pubsublite.googleapis.com":           {"", PersistenceStatic, true},
	"servicecontrol.googleapis.com":       {"", PersistenceStatic, true},
	"servicemanagement.googleapis.com":    {"", PersistenceStatic, true},
	"speech.googleapis.com":               {"", PersistenceStatic, true},
	"texttospeech.googleapis.com":         {"", PersistenceStatic, true},
	"storagetransfer.googleapis.com":      {"", PersistenceFile, true},
	"accesscontextmanager.googleapis.com": {"", PersistenceFile, true},
	"cloudtrace.googleapis.com":           {"", PersistenceFile, true},
	"clouderrorreporting.googleapis.com":  {"", PersistenceFile, true},
	"cloudasset.googleapis.com":           {"", PersistenceMemory, true},
	"dlp.googleapis.com":                  {"", PersistenceFile, true},
	"vision.googleapis.com":               {"", PersistenceMemory, true},
	"translate.googleapis.com":            {"", PersistenceMemory, true},
	"firebasehosting.googleapis.com":      {FidelityPassthrough, PersistenceDocker, true},
	"firebaseio.com":                      {FidelityPassthrough, PersistenceDocker, true},
	"firestore.googleapis.com":            {FidelityPassthrough, PersistenceDocker, false},
	"iam.googleapis.com":                  {FidelityStandard, PersistenceFile, true},
	"iamcredentials.googleapis.com":       {FidelityStandard, PersistenceStatic, true},
	"identityplatform.googleapis.com":     {"", PersistenceFile, true},
	"identitytoolkit.googleapis.com":      {FidelityPassthrough, PersistenceDocker, true},
	"logging.googleapis.com":              {FidelityStandard, PersistenceFile, true},
	"memcache.googleapis.com":             {FidelityStandard, PersistenceHybrid, true},
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

var deferredServiceContracts = map[string]string{}

var experimentalServiceContracts = map[string]bool{
	"accesscontextmanager.googleapis.com": true,
	"alloydb.googleapis.com":              true,
	"apigateway.googleapis.com":           true,
	"batch.googleapis.com":                true,
	"binaryauthorization.googleapis.com":  true,
	"cloudasset.googleapis.com":           true,
	"clouddeploy.googleapis.com":          true,
	"clouderrorreporting.googleapis.com":  true,
	"cloudprofiler.googleapis.com":        true,
	"cloudtrace.googleapis.com":           true,
	"composer.googleapis.com":             true,
	"dataflow.googleapis.com":             true,
	"dataform.googleapis.com":             true,
	"dlp.googleapis.com":                  true,
	"documentai.googleapis.com":           true,
	"dialogflow.googleapis.com":           true,
	"eventarc.googleapis.com":             true,
	"file.googleapis.com":                 true,
	"identityplatform.googleapis.com":     true,
	"language.googleapis.com":             true,
	"managedkafka.googleapis.com":         true,
	"networksecurity.googleapis.com":      true,
	"networkservices.googleapis.com":      true,
	"orgpolicy.googleapis.com":            true,
	"privateca.googleapis.com":            true,
	"pubsublite.googleapis.com":           true,
	"servicecontrol.googleapis.com":       true,
	"servicemanagement.googleapis.com":    true,
	"servicedirectory.googleapis.com":     true,
	"speech.googleapis.com":               true,
	"storagetransfer.googleapis.com":      true,
	"texttospeech.googleapis.com":         true,
	"translate.googleapis.com":            true,
	"vision.googleapis.com":               true,
	"workflows.googleapis.com":            true,
	"workflowexecutions.googleapis.com":   true,
}

var experimentalRouteContracts = map[string]func(*http.Request) bool{
	"aiplatform.googleapis.com": func(request *http.Request) bool {
		path := request.URL.Path
		if strings.HasPrefix(path, "/v1/internal/") ||
			strings.Contains(path, "/batchPredictionJobs") ||
			strings.Contains(path, "/featurestores") {
			return false
		}
		if strings.Contains(path, "/endpoints/") && strings.HasSuffix(path, ":predict") {
			return false
		}
		if strings.Contains(path, "/publishers/") && strings.Contains(path, "/models/") &&
			(strings.HasSuffix(path, ":predict") ||
				strings.HasSuffix(path, ":generateContent") ||
				strings.HasSuffix(path, ":streamGenerateContent")) {
			return false
		}
		return strings.Contains(path, "/indexes") ||
			strings.Contains(path, "/indexEndpoints") ||
			strings.Contains(path, "/models") ||
			strings.Contains(path, "/operations/")
	},
}

func experimentalServicesEnabled() bool {
	return os.Getenv(ExperimentalServicesEnv) == "1"
}

// IsExperimentalService reports whether domain is default-gated experimental
// surface.
func IsExperimentalService(domain string) bool {
	return experimentalServiceContracts[domain]
}

type experimentalGateHandler struct {
	domain string
}

func (handler *experimentalGateHandler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotImplemented)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"code": http.StatusNotImplemented,
			"message": "MiniSky: " + handler.domain + " is experimental and disabled; set " +
				ExperimentalServicesEnv +
				"=1 to opt in; promotion evidence is incomplete",
			"status": "UNIMPLEMENTED",
		},
	})
}

func experimentalDisabledHandler(domain string) http.Handler {
	return &experimentalGateHandler{domain: domain}
}

// GateExperimentalRequest writes the default-off response for a route-level
// experimental surface and reports whether dispatch must stop.
func GateExperimentalRequest(w http.ResponseWriter, request *http.Request, domain string) bool {
	route := experimentalRouteContracts[domain]
	if route == nil || experimentalServicesEnabled() || !route(request) {
		return false
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotImplemented)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"code": http.StatusNotImplemented,
			"message": "MiniSky: this " + domain + " route is experimental and disabled; set " +
				ExperimentalServicesEnv + "=1 to opt in; promotion evidence is incomplete",
			"status": "UNIMPLEMENTED",
		},
	})
	return true
}

// IsExperimentalDisabled reports whether a boot-time handler is the default-off
// experimental gate. Routers use this marker before validation and IAM so every
// request to a disabled experimental domain receives the same 501 contract.
func IsExperimentalDisabled(handler http.Handler) bool {
	_, disabled := handler.(*experimentalGateHandler)
	return disabled
}

// backendContracts records the Docker boundary for services whose persistence
// includes an executable backend. Pure passthrough domains require a successful
// cold start; hybrid domains persist metadata while owning bounded Docker work.
var backendContracts = map[string]string{
	"alloydb.googleapis.com":      "AlloyDB metadata is profile-persisted; primary instances use exact-owned PostgreSQL containers",
	"batch.googleapis.com":        "Batch job metadata is profile-persisted; bounded container runnables execute in exact-owned digest-pinned Docker containers",
	"composer.googleapis.com":     "Composer metadata is profile-persisted; environments use exact-owned Airflow containers",
	"datastore.googleapis.com":    "Google Cloud Datastore emulator; cold-start and backend errors are deterministic, CRUD requires Docker",
	"firestore.googleapis.com":    "Google Cloud Firestore emulator; cold-start and backend errors are deterministic, CRUD requires Docker",
	"managedkafka.googleapis.com": "Managed Kafka metadata is profile-persisted; clusters use exact-owned Kafka containers",
	"memcache.googleapis.com":     "Memcached metadata is profile-persisted; instances use exact-owned Memcached containers without durable cache contents",
	"redis.googleapis.com":        "Memorystore metadata is profile-persisted; instances use exact-owned Valkey containers and volumes",
	"spanner.googleapis.com":      "Cloud Spanner emulator; cold-start and backend errors are deterministic, database behavior requires Docker",
}

var dockerOperationContracts = map[string][]DockerOperation{
	"alloydb.googleapis.com": {
		{HTTPMethod: http.MethodPost, PathGlob: "/v1/projects/*/locations/*/clusters/*/instances", RequestBody: DockerRequestBodyAlloyDBPrimary},
		{HTTPMethod: http.MethodDelete, PathGlob: "/v1/projects/*/locations/*/clusters/*/instances/*"},
	},
	"batch.googleapis.com": {
		{HTTPMethod: http.MethodPost, PathGlob: "/v1/projects/*/locations/*/jobs", RequestBody: DockerRequestBodyBatchRunnable},
	},
	"composer.googleapis.com": {
		{HTTPMethod: http.MethodPost, PathGlob: "/v1/projects/*/locations/*/environments"},
		{HTTPMethod: http.MethodDelete, PathGlob: "/v1/projects/*/locations/*/environments/*"},
	},
	"managedkafka.googleapis.com": {
		{HTTPMethod: http.MethodPost, PathGlob: "/v1/projects/*/locations/*/clusters"},
		{HTTPMethod: http.MethodDelete, PathGlob: "/v1/projects/*/locations/*/clusters/*"},
	},
	"redis.googleapis.com": {
		{HTTPMethod: http.MethodPost, PathGlob: "/v1/projects/*/locations/*/instances"},
		{HTTPMethod: http.MethodDelete, PathGlob: "/v1/projects/*/locations/*/instances/*"},
	},
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
		supportReason := deferredServiceContracts[domain]
		if supportReason != "" {
			support = SupportDeferred
		} else if experimentalServiceContracts[domain] {
			support = SupportExperimental
			supportReason = "Experimental handler is default-off while promotion evidence remains incomplete"
		}
		services = append(services, Service{
			Domain:           domain,
			Support:          support,
			Fidelity:         metadata.fidelity,
			Persistence:      metadata.persistence,
			LazyDocker:       lazy,
			ProbeUnsupported: metadata.probeUnsupported,
			BackendContract:  backendContracts[domain],
			DockerOperations: append([]DockerOperation(nil), dockerOperationContracts[domain]...),
			SupportReason:    supportReason,
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
		if service.Support == SupportExperimental && !experimentalServicesEnabled() {
			ctx.shims[service.Domain] = experimentalDisabledHandler(service.Domain)
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
		if experimentalServiceContracts[domain] && !experimentalServicesEnabled() {
			handlers[domain] = handler
		} else {
			handlers[domain] = ContractHandler(domain, handler)
		}
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

// RuntimeHandler applies the executable service contract for one boot. Hybrid
// factories remain concrete during construction, while Docker-dependent
// mutations are rejected centrally when that boot has no Docker backend.
func RuntimeHandler(domain string, next http.Handler, dockerAvailable bool) http.Handler {
	if IsExperimentalDisabled(next) {
		return next
	}
	contract := ContractHandler(domain, next)
	if dockerAvailable {
		return contract
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requiresDockerMutation(domain, r) {
			WriteDockerUnavailable(w)
			return
		}
		contract.ServeHTTP(w, r)
	})
}
