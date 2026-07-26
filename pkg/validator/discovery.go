package validator

// ─────────────────────────────────────────────────────────────────────────────
// Embedded GCP Discovery Rules
//
// Each entry represents a critical creation/mutation endpoint.
// Read/list/delete endpoints are intentionally less strict (they have no body).
// Rules are derived from the following Discovery Documents:
//   compute  v1  → https://discovery.googleapis.com/discovery/v1/apis/compute/v1/rest
//   sqladmin v1  → https://discovery.googleapis.com/discovery/v1/apis/sqladmin/v1/rest
//   iam      v1  → https://discovery.googleapis.com/discovery/v1/apis/iam/v1/rest
//   bigquery v2  → https://discovery.googleapis.com/discovery/v1/apis/bigquery/v2/rest
//   container v1 → https://discovery.googleapis.com/discovery/v1/apis/container/v1/rest
//   dataproc  v1 → https://discovery.googleapis.com/discovery/v1/apis/dataproc/v1/rest
//   dns       v1 → https://discovery.googleapis.com/discovery/v1/apis/dns/v1/rest
//   run       v2 → https://discovery.googleapis.com/discovery/v1/apis/run/v2/rest
//   cloudfunctions v2 → https://discovery.googleapis.com/discovery/v1/apis/cloudfunctions/v2/rest
// ─────────────────────────────────────────────────────────────────────────────

// embeddedRules is the full set of Discovery-Doc-derived validation rules.
// One entry per service domain.
var embeddedRules = []ServiceSchema{
	{
		Domain: "cloudresourcemanager.googleapis.com",
		Methods: []MethodSchema{{
			HTTPMethod:  "POST",
			PathGlob:    "/v3/projects",
			ContentType: "application/json",
			RequiredBody: []BodyField{{
				Path: "projectId", Type: "string",
				Message: "field 'projectId' is required for projects.create",
			}},
		}, {
			HTTPMethod:  "POST",
			PathGlob:    "/v3/folders",
			ContentType: "application/json",
			RequiredBody: []BodyField{{
				Path: "parent", Type: "string",
				Message: "field 'parent' is required for folders.create",
			}},
		}},
	},

	// ── Compute Engine ──────────────────────────────────────────────────────
	{
		Domain: "compute.googleapis.com",
		Methods: []MethodSchema{

			// instances.insert — requires `name` in the body
			{
				HTTPMethod:  "POST",
				PathGlob:    "/compute/v1/projects/*/zones/*/instances",
				ContentType: "application/json",
				RequiredBody: []BodyField{
					{Path: "name", Type: "string",
						Message: "field 'name' is required for instances.insert"},
				},
			},

			// networks.insert — requires `name`
			{
				HTTPMethod:  "POST",
				PathGlob:    "/compute/v1/projects/*/global/networks",
				ContentType: "application/json",
				RequiredBody: []BodyField{
					{Path: "name", Type: "string",
						Message: "field 'name' is required for networks.insert"},
				},
			},

			// subnetworks.insert — requires the bounded primary IPv4 contract
			{
				HTTPMethod:   "POST",
				PathGlob:     "/compute/v1/projects/*/regions/*/subnetworks",
				ContentType:  "application/json",
				MaxBodyBytes: 1 << 20,
				RequiredBody: []BodyField{
					{Path: "name", Type: "string",
						Message: "field 'name' is required for subnetworks.insert"},
					{Path: "ipCidrRange", Type: "string",
						Message: "field 'ipCidrRange' is required for subnetworks.insert"},
					{Path: "network", Type: "string",
						Message: "field 'network' is required for subnetworks.insert"},
				},
			},

			// securityPolicies.insert — requires `name`
			{
				HTTPMethod:  "POST",
				PathGlob:    "/compute/v1/projects/*/global/securityPolicies",
				ContentType: "application/json",
				RequiredBody: []BodyField{
					{Path: "name", Type: "string",
						Message: "field 'name' is required for securityPolicies.insert"},
				},
			},
		},
	},

	// ── Cloud SQL (sqladmin) ─────────────────────────────────────────────────
	{
		Domain: "sqladmin.googleapis.com",
		Methods: []MethodSchema{

			// sql.instances.insert — requires `name`
			{
				HTTPMethod:  "POST",
				PathGlob:    "/v1/projects/*/instances",
				ContentType: "application/json",
				RequiredBody: []BodyField{
					{Path: "name", Type: "string",
						Message: "field 'name' is required for sql.instances.insert"},
				},
			},

			// sql.databases.insert — requires `name`
			{
				HTTPMethod:  "POST",
				PathGlob:    "/v1/projects/*/instances/*/databases",
				ContentType: "application/json",
				RequiredBody: []BodyField{
					{Path: "name", Type: "string",
						Message: "field 'name' is required for sql.databases.insert"},
				},
			},

			// sql.users.insert — requires `name`
			{
				HTTPMethod:  "POST",
				PathGlob:    "/v1/projects/*/instances/*/users",
				ContentType: "application/json",
				RequiredBody: []BodyField{
					{Path: "name", Type: "string",
						Message: "field 'name' is required for sql.users.insert"},
				},
			},
		},
	},

	// ── IAM ─────────────────────────────────────────────────────────────────
	{
		Domain: "iam.googleapis.com",
		Methods: []MethodSchema{

			// serviceAccounts.create — requires `accountId`
			{
				HTTPMethod:  "POST",
				PathGlob:    "/v1/projects/*/serviceAccounts",
				ContentType: "application/json",
				RequiredBody: []BodyField{
					{Path: "accountId", Type: "string",
						Message: "field 'accountId' is required for serviceAccounts.create"},
				},
			},
		},
	},

	// ── BigQuery ─────────────────────────────────────────────────────────────
	{
		Domain: "bigquery.googleapis.com",
		Methods: []MethodSchema{

			// datasets.insert — requires datasetReference.datasetId
			{
				HTTPMethod:  "POST",
				PathGlob:    "/bigquery/v2/projects/*/datasets",
				ContentType: "application/json",
				RequiredBody: []BodyField{
					{Path: "datasetReference.datasetId", Type: "string",
						Message: "field 'datasetReference.datasetId' is required for datasets.insert"},
				},
			},

			// tables.insert — requires tableReference.tableId
			{
				HTTPMethod:  "POST",
				PathGlob:    "/bigquery/v2/projects/*/datasets/*/tables",
				ContentType: "application/json",
				RequiredBody: []BodyField{
					{Path: "tableReference.tableId", Type: "string",
						Message: "field 'tableReference.tableId' is required for tables.insert"},
				},
			},

			// jobs.insert — requires configuration.jobType
			{
				HTTPMethod:  "POST",
				PathGlob:    "/bigquery/v2/projects/*/jobs",
				ContentType: "application/json",
				RequiredBody: []BodyField{
					{Path: "configuration", Type: "object",
						Message: "field 'configuration' is required for jobs.insert"},
				},
			},

			// tabledata.insertAll — requires `rows`
			{
				HTTPMethod:  "POST",
				PathGlob:    "/bigquery/v2/projects/*/datasets/*/tables/*/insertAll",
				ContentType: "application/json",
				RequiredBody: []BodyField{
					{Path: "rows", Type: "array",
						Message: "field 'rows' is required for tabledata.insertAll"},
				},
			},
		},
	},

	// ── GKE (container) ──────────────────────────────────────────────────────
	{
		Domain: "container.googleapis.com",
		Methods: []MethodSchema{

			// clusters.create (zone-based) — requires cluster.name
			{
				HTTPMethod:  "POST",
				PathGlob:    "/v1/projects/*/zones/*/clusters",
				ContentType: "application/json",
				RequiredBody: []BodyField{
					{Path: "cluster.name", Type: "string",
						Message: "field 'cluster.name' is required for clusters.create"},
				},
			},

			// clusters.create (location-based) — same requirement
			{
				HTTPMethod:  "POST",
				PathGlob:    "/v1/projects/*/locations/*/clusters",
				ContentType: "application/json",
				RequiredBody: []BodyField{
					{Path: "cluster.name", Type: "string",
						Message: "field 'cluster.name' is required for clusters.create"},
				},
			},
		},
	},

	// ── Dataproc ─────────────────────────────────────────────────────────────
	{
		Domain: "dataproc.googleapis.com",
		Methods: []MethodSchema{

			// clusters.create — requires clusterName
			{
				HTTPMethod:  "POST",
				PathGlob:    "/v1/projects/*/regions/*/clusters",
				ContentType: "application/json",
				RequiredBody: []BodyField{
					{Path: "clusterName", Type: "string",
						Message: "field 'clusterName' is required for clusters.create"},
				},
			},

			// jobs.submit — requires placement.clusterName
			{
				HTTPMethod:  "POST",
				PathGlob:    "/v1/projects/*/regions/*/jobs",
				ContentType: "application/json",
				RequiredBody: []BodyField{
					{Path: "job.placement.clusterName", Type: "string",
						Message: "field 'job.placement.clusterName' is required for jobs.submit"},
				},
			},
		},
	},

	// ── Cloud DNS ────────────────────────────────────────────────────────────
	{
		Domain: "dns.googleapis.com",
		Methods: []MethodSchema{

			// managedZones.create — requires name and dnsName
			{
				HTTPMethod:  "POST",
				PathGlob:    "/dns/v1/projects/*/managedZones",
				ContentType: "application/json",
				RequiredBody: []BodyField{
					{Path: "name", Type: "string",
						Message: "field 'name' is required for managedZones.create"},
					{Path: "dnsName", Type: "string",
						Message: "field 'dnsName' is required for managedZones.create"},
				},
			},

			// resourceRecordSets.create — requires name and type
			{
				HTTPMethod:  "POST",
				PathGlob:    "/dns/v1/projects/*/managedZones/*/rrsets",
				ContentType: "application/json",
				RequiredBody: []BodyField{
					{Path: "name", Type: "string",
						Message: "field 'name' (FQDN) is required for resourceRecordSets.create"},
					{Path: "type", Type: "string",
						Message: "field 'type' (e.g. A, CNAME, MX) is required for resourceRecordSets.create"},
				},
			},

			// changes.create — requires at least additions or deletions
			{
				HTTPMethod:  "POST",
				PathGlob:    "/dns/v1/projects/*/managedZones/*/changes",
				ContentType: "application/json",
				// No specific body field required — either additions or deletions must be present,
				// but the DNS shim handles that logic itself.
			},
		},
	},

	// ── Cloud Functions ───────────────────────────────────────────────────────
	{
		Domain: "cloudfunctions.googleapis.com",
		Methods: []MethodSchema{

			// functions.create — requires functionId query param
			{
				HTTPMethod:    "POST",
				PathGlob:      "/v2/projects/*/locations/*/functions",
				ContentType:   "application/json",
				RequiredQuery: []string{"functionId"},
			},
		},
	},

	// ── Cloud Run ─────────────────────────────────────────────────────────────
	{
		Domain: "run.googleapis.com",
		Methods: []MethodSchema{

			// services.create — requires serviceId query param
			{
				HTTPMethod:    "POST",
				PathGlob:      "/v2/projects/*/locations/*/services",
				ContentType:   "application/json",
				RequiredQuery: []string{"serviceId"},
			},
		},
	},

	// ── Cloud Storage ────────────────────────────────────────────────────────
	{
		Domain: "storage.googleapis.com",
		Methods: []MethodSchema{
			{
				HTTPMethod:  "POST",
				PathGlob:    "/storage/v1/b",
				ContentType: "application/json",
				RequiredBody: []BodyField{
					{Path: "name", Type: "string",
						Message: "field 'name' is required for buckets.insert"},
				},
			},
		},
	},

	// ── Pub/Sub ──────────────────────────────────────────────────────────────
	{
		Domain: "pubsub.googleapis.com",
		Methods: []MethodSchema{
			{
				HTTPMethod:  "PUT",
				PathGlob:    "/v1/projects/*/subscriptions/*",
				ContentType: "application/json",
				RequiredBody: []BodyField{
					{Path: "topic", Type: "string",
						Message: "field 'topic' is required for subscriptions.create"},
				},
			},
			// The Pub/Sub shim also accepts the emulator-style path and adds /v1.
			{
				HTTPMethod:  "PUT",
				PathGlob:    "/projects/*/subscriptions/*",
				ContentType: "application/json",
				RequiredBody: []BodyField{
					{Path: "topic", Type: "string",
						Message: "field 'topic' is required for subscriptions.create"},
				},
			},
		},
	},

	// ── Secret Manager ───────────────────────────────────────────────────────
	{
		Domain: "secretmanager.googleapis.com",
		Methods: []MethodSchema{
			{
				HTTPMethod:    "POST",
				PathGlob:      "/v1/projects/*/secrets",
				ContentType:   "application/json",
				RequiredQuery: []string{"secretId"},
				RequiredBody: []BodyField{
					{Path: "replication", Type: "object",
						Message: "field 'replication' is required for secrets.create"},
				},
			},
			{
				HTTPMethod:  "POST",
				PathGlob:    "/v1/projects/*/secrets/*",
				ContentType: "application/json",
				RequiredBody: []BodyField{
					{Path: "payload.data", Type: "string",
						Message: "field 'payload.data' is required for secretVersions.add"},
				},
			},
		},
	},

	// ── Cloud KMS ────────────────────────────────────────────────────────────
	{
		Domain: "cloudkms.googleapis.com",
		Methods: []MethodSchema{
			{
				HTTPMethod:    "POST",
				PathGlob:      "/v1/projects/*/locations/*/keyRings",
				ContentType:   "application/json",
				RequiredQuery: []string{"keyRingId"},
			},
			{
				HTTPMethod:    "POST",
				PathGlob:      "/v1/projects/*/locations/*/keyRings/*/cryptoKeys",
				ContentType:   "application/json",
				RequiredQuery: []string{"cryptoKeyId"},
			},
		},
	},

	// ── Cloud Scheduler ──────────────────────────────────────────────────────
	{
		Domain: "cloudscheduler.googleapis.com",
		Methods: []MethodSchema{
			{
				HTTPMethod:  "POST",
				PathGlob:    "/v1/projects/*/locations/*/jobs",
				ContentType: "application/json",
				RequiredBody: []BodyField{
					{Path: "name", Type: "string",
						Message: "field 'name' is required for jobs.create"},
					{Path: "schedule", Type: "string",
						Message: "field 'schedule' is required for jobs.create"},
				},
			},
		},
	},

	// ── Cloud Tasks ──────────────────────────────────────────────────────────
	{
		Domain: "cloudtasks.googleapis.com",
		Methods: []MethodSchema{
			{
				HTTPMethod:  "POST",
				PathGlob:    "/v2/projects/*/locations/*/queues",
				ContentType: "application/json",
				RequiredBody: []BodyField{
					{Path: "name", Type: "string",
						Message: "field 'name' is required for queues.create"},
				},
			},
			{
				HTTPMethod:  "POST",
				PathGlob:    "/v2/projects/*/locations/*/queues/*/tasks",
				ContentType: "application/json",
				RequiredBody: []BodyField{
					{Path: "task", Type: "object",
						Message: "field 'task' is required for tasks.create"},
				},
			},
		},
	},

	// ── Cloud Build ──────────────────────────────────────────────────────────
	{
		Domain: "cloudbuild.googleapis.com",
		Methods: []MethodSchema{
			{
				HTTPMethod:  "POST",
				PathGlob:    "/v1/projects/*/builds",
				ContentType: "application/json",
				RequiredBody: []BodyField{
					{Path: "steps", Type: "array",
						Message: "field 'steps' is required for builds.create"},
				},
			},
			{
				HTTPMethod:  "POST",
				PathGlob:    "/v1/projects/*/locations/*/builds",
				ContentType: "application/json",
				RequiredBody: []BodyField{
					{Path: "steps", Type: "array",
						Message: "field 'steps' is required for builds.create"},
				},
			},
		},
	},

	// ── Artifact Registry ────────────────────────────────────────────────────
	{
		Domain: "artifactregistry.googleapis.com",
		Methods: []MethodSchema{
			{
				HTTPMethod:    "POST",
				PathGlob:      "/v1/projects/*/locations/*/repositories",
				ContentType:   "application/json",
				RequiredQuery: []string{"repositoryId"},
				RequiredBody: []BodyField{
					{Path: "format", Type: "string",
						Message: "field 'format' is required for repositories.create"},
				},
			},
		},
	},
	{
		Domain: "redis.googleapis.com",
		Methods: []MethodSchema{{
			HTTPMethod:    "POST",
			PathGlob:      "/v1/projects/*/locations/*/instances",
			ContentType:   "application/json",
			RequiredQuery: []string{"instanceId"},
			RequiredBody: []BodyField{{
				Path: "memorySizeGb", Type: "integer",
				Message: "field 'memorySizeGb' is required for instances.create",
			}},
		}},
	},
	{
		Domain: "monitoring.googleapis.com",
		Methods: []MethodSchema{
			{
				HTTPMethod:  "POST",
				PathGlob:    "/v3/projects/*/metricDescriptors",
				ContentType: "application/json",
				RequiredBody: []BodyField{
					{Path: "type", Type: "string", Message: "field 'type' is required for metricDescriptors.create"},
					{Path: "metricKind", Type: "string", Message: "field 'metricKind' is required for metricDescriptors.create"},
					{Path: "valueType", Type: "string", Message: "field 'valueType' is required for metricDescriptors.create"},
				},
			},
			{
				HTTPMethod:  "POST",
				PathGlob:    "/v3/projects/*/timeSeries",
				ContentType: "application/json",
				RequiredBody: []BodyField{{
					Path: "timeSeries", Type: "array",
					Message: "field 'timeSeries' is required for timeSeries.create",
				}},
			},
		},
	},
	{
		Domain: "logging.googleapis.com",
		Methods: []MethodSchema{
			{
				HTTPMethod:  "POST",
				PathGlob:    "/v2/entries:write",
				ContentType: "application/json",
				RequiredBody: []BodyField{{
					Path: "entries", Type: "array",
					Message: "field 'entries' is required for entries.write",
				}},
			},
			{
				HTTPMethod:  "POST",
				PathGlob:    "/v2/projects/*/sinks",
				ContentType: "application/json",
				RequiredBody: []BodyField{
					{Path: "name", Type: "string", Message: "field 'name' is required for sinks.create"},
					{Path: "destination", Type: "string", Message: "field 'destination' is required for sinks.create"},
				},
			},
		},
	},
	{
		Domain: "aiplatform.googleapis.com",
		Methods: []MethodSchema{
			{
				HTTPMethod:  "POST",
				PathGlob:    "/v1/projects/*/locations/*/publishers/*/models/*:generateContent",
				ContentType: "application/json",
				RequiredBody: []BodyField{{
					Path: "contents", Type: "array",
					Message: "field 'contents' is required for models.generateContent",
				}},
			},
			{
				HTTPMethod:  "POST",
				PathGlob:    "/v1/projects/*/locations/*/endpoints/*:predict",
				ContentType: "application/json",
				RequiredBody: []BodyField{{
					Path: "instances", Type: "array",
					Message: "field 'instances' is required for endpoints.predict",
				}},
			},
		},
	},
}
