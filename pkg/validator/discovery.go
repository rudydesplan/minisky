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
				HTTPMethod:   "POST",
				PathGlob:     "/bigquery/v2/projects/*/datasets/*/tables/*/insertAll",
				ContentType:  "application/json",
				MaxBodyBytes: 10 << 20,
				RequiredBody: []BodyField{
					{Path: "rows", Type: "array",
						Message:  "field 'rows' is required for tabledata.insertAll",
						MaxItems: 50_000},
				},
			},
			{
				HTTPMethod:   "POST",
				PathGlob:     "/upload/bigquery/v2/projects/*/jobs",
				MaxBodyBytes: 50 << 20,
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
			{
				HTTPMethod:   "POST",
				PathGlob:     "/upload/storage/v1/b/*/o",
				MaxBodyBytes: 64 << 20,
			},
			{
				HTTPMethod:   "PUT",
				PathGlob:     "/upload/storage/v1/b/*/o",
				MaxBodyBytes: 64 << 20,
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
			{
				HTTPMethod:   "POST",
				PathGlob:     "/v1/projects/*/topics/*:publish",
				ContentType:  "application/json",
				MaxBodyBytes: 10 << 20,
				RequiredBody: []BodyField{{
					Path: "messages", Type: "array",
					Message:  "field 'messages' is required for topics.publish",
					MaxItems: 1_000,
				}},
			},
			{
				HTTPMethod:   "POST",
				PathGlob:     "/projects/*/topics/*:publish",
				ContentType:  "application/json",
				MaxBodyBytes: 10 << 20,
				RequiredBody: []BodyField{{
					Path: "messages", Type: "array",
					Message:  "field 'messages' is required for topics.publish",
					MaxItems: 1_000,
				}},
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
						Message:         "field 'payload.data' is required for secretVersions.add",
						MaxDecodedBytes: 64 << 10},
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
					Message:  "field 'entries' is required for entries.write",
					MaxItems: 1_000,
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
				HTTPMethod:   "POST",
				PathGlob:     "/v1/projects/*/locations/*/publishers/*/models/*:generateContent",
				ContentType:  "application/json",
				MaxBodyBytes: 4 << 20,
				RequiredBody: []BodyField{{
					Path: "contents", Type: "array",
					Message: "field 'contents' is required for models.generateContent",
				}},
			},
			{
				HTTPMethod:   "POST",
				PathGlob:     "/v1/projects/*/locations/*/endpoints/*:predict",
				ContentType:  "application/json",
				MaxBodyBytes: 4 << 20,
				RequiredBody: []BodyField{{
					Path: "instances", Type: "array",
					Message: "field 'instances' is required for endpoints.predict",
				}},
			},
			{
				HTTPMethod:  "POST",
				PathGlob:    "/v1/projects/*/locations/*/indexes",
				ContentType: "application/json",
				RequiredBody: []BodyField{{
					Path: "displayName", Type: "string",
					Message: "field 'displayName' is required for indexes.create",
				}},
			},
			{
				HTTPMethod:  "POST",
				PathGlob:    "/v1/projects/*/locations/*/models:upload",
				ContentType: "application/json",
				RequiredBody: []BodyField{{
					Path: "model.displayName", Type: "string",
					Message: "field 'model.displayName' is required for models.upload",
				}},
			},
		},
	},

	// Phase 18
	{Domain: "eventarc.googleapis.com", Methods: []MethodSchema{
		{HTTPMethod: "POST", PathGlob: "/v1/projects/*/locations/*/triggers", ContentType: "application/json",
			RequiredBody:  []BodyField{{Path: "eventFilters", Type: "array", Message: "field 'eventFilters' is required for triggers.create"}},
			RequiredQuery: []string{"triggerId"}},
	}},
	{Domain: "workflows.googleapis.com", Methods: []MethodSchema{
		{HTTPMethod: "POST", PathGlob: "/v1/projects/*/locations/*/workflows", ContentType: "application/json",
			RequiredBody:  []BodyField{{Path: "sourceContents", Type: "string", Message: "field 'sourceContents' is required for workflows.create"}},
			RequiredQuery: []string{"workflowId"}},
	}},
	{Domain: "batch.googleapis.com", Methods: []MethodSchema{
		{HTTPMethod: "POST", PathGlob: "/v1/projects/*/locations/*/jobs", ContentType: "application/json",
			RequiredBody: []BodyField{{Path: "taskGroups", Type: "array", Message: "field 'taskGroups' is required for jobs.create"}}},
	}},
	// Phase 19
	{Domain: "dataflow.googleapis.com", Methods: []MethodSchema{
		{HTTPMethod: "POST", PathGlob: "/v1b3/projects/*/locations/*/jobs", ContentType: "application/json"},
	}},
	{Domain: "composer.googleapis.com", Methods: []MethodSchema{
		{HTTPMethod: "POST", PathGlob: "/v1/projects/*/locations/*/environments", ContentType: "application/json",
			RequiredBody: []BodyField{{Path: "name", Type: "string", Message: "field 'name' is required for environments.create"}}},
	}},
	{Domain: "managedkafka.googleapis.com", Methods: []MethodSchema{
		{HTTPMethod: "POST", PathGlob: "/v1/projects/*/locations/*/clusters", ContentType: "application/json",
			RequiredQuery: []string{"clusterId"}},
		{HTTPMethod: "POST", PathGlob: "/v1/projects/*/locations/*/clusters/*/topics", ContentType: "application/json",
			RequiredQuery: []string{"topicId"}},
	}},
	{Domain: "dataform.googleapis.com", Methods: []MethodSchema{
		{HTTPMethod: "POST", PathGlob: "/v1beta1/projects/*/locations/*/repositories", ContentType: "application/json",
			RequiredQuery: []string{"repositoryId"}},
	}},
	// Phase 20
	{Domain: "alloydb.googleapis.com", Methods: []MethodSchema{
		{HTTPMethod: "POST", PathGlob: "/v1/projects/*/locations/*/clusters", ContentType: "application/json",
			RequiredQuery: []string{"clusterId"}},
	}},
	{Domain: "identityplatform.googleapis.com", Methods: []MethodSchema{
		{HTTPMethod: "POST", PathGlob: "/v2/projects/*/tenants", ContentType: "application/json",
			RequiredBody: []BodyField{{Path: "displayName", Type: "string", Message: "field 'displayName' is required for tenants.create"}}},
	}},
	{Domain: "file.googleapis.com", Methods: []MethodSchema{
		{HTTPMethod: "POST", PathGlob: "/v1/projects/*/locations/*/instances", ContentType: "application/json",
			RequiredQuery: []string{"instanceId"},
			RequiredBody: []BodyField{
				{Path: "tier", Type: "string", Message: "field 'tier' is required for instances.create"},
				{Path: "fileShares", Type: "array", Message: "field 'fileShares' is required for instances.create"},
				{Path: "networks", Type: "array", Message: "field 'networks' is required for instances.create"},
			}},
	}},
	{Domain: "storagetransfer.googleapis.com", Methods: []MethodSchema{
		{HTTPMethod: "POST", PathGlob: "/v1/transferJobs", ContentType: "application/json",
			RequiredBody: []BodyField{
				{Path: "projectId", Type: "string", Message: "field 'projectId' is required for transferJobs.create"},
				{Path: "transferSpec", Type: "object", Message: "field 'transferSpec' is required for transferJobs.create"},
			}},
	}},
	// Phase 21
	{Domain: "cloudtrace.googleapis.com", Methods: []MethodSchema{
		{HTTPMethod: "POST", PathGlob: "/v2/projects/*/traces:batchWrite", ContentType: "application/json",
			RequiredBody: []BodyField{{Path: "spans", Type: "array", Message: "field 'spans' is required for traces.batchWrite"}}},
	}},
	{Domain: "clouderrorreporting.googleapis.com", Methods: []MethodSchema{
		{HTTPMethod: "POST", PathGlob: "/v1beta1/projects/*/events:report", ContentType: "application/json",
			RequiredBody: []BodyField{{Path: "message", Type: "string", Message: "field 'message' is required for events.report"}}},
	}},
	{Domain: "cloudprofiler.googleapis.com", Methods: []MethodSchema{
		{HTTPMethod: "POST", PathGlob: "/v2/projects/*/profiles", ContentType: "application/json",
			RequiredBody: []BodyField{{Path: "profileType", Type: "array", Message: "field 'profileType' is required for profiles.create"}}},
	}},
	// Phase 22
	{Domain: "apigateway.googleapis.com", Methods: []MethodSchema{
		{HTTPMethod: "POST", PathGlob: "/v1/projects/*/locations/*/apis", ContentType: "application/json",
			RequiredQuery: []string{"apiId"}},
	}},
	{Domain: "servicedirectory.googleapis.com", Methods: []MethodSchema{
		{HTTPMethod: "POST", PathGlob: "/v1/projects/*/locations/*/namespaces/*/services/*/endpoints", ContentType: "application/json",
			RequiredBody: []BodyField{{Path: "address", Type: "string", Message: "field 'address' is required for endpoints.create"}}},
	}},
	{Domain: "clouddeploy.googleapis.com", Methods: []MethodSchema{
		{HTTPMethod: "POST", PathGlob: "/v1/projects/*/locations/*/deliveryPipelines", ContentType: "application/json",
			RequiredQuery: []string{"deliveryPipelineId"}},
	}},
	// Phase 23
	{Domain: "vision.googleapis.com", Methods: []MethodSchema{
		{HTTPMethod: "POST", PathGlob: "/v1/images:annotate", ContentType: "application/json",
			RequiredBody: []BodyField{{Path: "requests", Type: "array", Message: "field 'requests' is required for images.annotate"}}},
	}},
	{Domain: "translate.googleapis.com", Methods: []MethodSchema{
		{HTTPMethod: "POST", PathGlob: "/v3/projects/*/locations/*:translateText", ContentType: "application/json",
			RequiredBody: []BodyField{
				{Path: "contents", Type: "array", Message: "field 'contents' is required for translateText"},
				{Path: "targetLanguageCode", Type: "string", Message: "field 'targetLanguageCode' is required for translateText"},
			}},
	}},
	{Domain: "documentai.googleapis.com", Methods: []MethodSchema{
		{HTTPMethod: "POST", PathGlob: "/v1/projects/*/locations/*/processors", ContentType: "application/json",
			RequiredBody: []BodyField{{Path: "type", Type: "string", Message: "field 'type' is required for processors.create"}}},
	}},
	// Phase 24
	{Domain: "dlp.googleapis.com", Methods: []MethodSchema{
		{HTTPMethod: "POST", PathGlob: "/v2/projects/*/inspectTemplates", ContentType: "application/json",
			RequiredBody: []BodyField{{Path: "inspectTemplate", Type: "object", Message: "field 'inspectTemplate' is required for inspectTemplates.create"}}},
	}},
	{Domain: "orgpolicy.googleapis.com", Methods: []MethodSchema{
		{HTTPMethod: "POST", PathGlob: "/v2/projects/*/policies", ContentType: "application/json",
			RequiredBody: []BodyField{{Path: "spec", Type: "object", Message: "field 'spec' is required for policies.create"}}},
	}},
	// Phase 25
	{Domain: "networksecurity.googleapis.com", Methods: []MethodSchema{
		{HTTPMethod: "POST", PathGlob: "/v1/projects/*/locations/*/authorizationPolicies", ContentType: "application/json",
			RequiredQuery: []string{"authorizationPolicyId"},
			RequiredBody:  []BodyField{{Path: "name", Type: "string", Message: "field 'name' is required for authorizationPolicies.create"}}},
		{HTTPMethod: "POST", PathGlob: "/v1/projects/*/locations/*/authorizationPolicies:evaluate", ContentType: "application/json",
			RequiredBody: []BodyField{
				{Path: "project", Type: "string", Message: "field 'project' is required for authorizationPolicies.evaluate"},
				{Path: "location", Type: "string", Message: "field 'location' is required for authorizationPolicies.evaluate"},
			}},
	}},
	{Domain: "accesscontextmanager.googleapis.com", Methods: []MethodSchema{
		{HTTPMethod: "POST", PathGlob: "/v1/accessPolicies", ContentType: "application/json",
			RequiredBody: []BodyField{{Path: "title", Type: "string", Message: "field 'title' is required for accessPolicies.create"}}},
		{HTTPMethod: "POST", PathGlob: "/v1/accessPolicies/*:checkAccess", ContentType: "application/json",
			RequiredBody: []BodyField{
				{Path: "project", Type: "string", Message: "field 'project' is required for accessPolicies.checkAccess"},
				{Path: "service", Type: "string", Message: "field 'service' is required for accessPolicies.checkAccess"},
			}},
	}},
	{Domain: "networkservices.googleapis.com", Methods: []MethodSchema{
		{HTTPMethod: "POST", PathGlob: "/v1/projects/*/locations/*/meshes", ContentType: "application/json",
			RequiredQuery: []string{"meshId"},
			RequiredBody:  []BodyField{{Path: "name", Type: "string", Message: "field 'name' is required for meshes.create"}}},
		{HTTPMethod: "POST", PathGlob: "/v1/projects/*/locations/*/httpRoutes:resolve", ContentType: "application/json",
			RequiredBody: []BodyField{
				{Path: "project", Type: "string", Message: "field 'project' is required for httpRoutes.resolve"},
				{Path: "location", Type: "string", Message: "field 'location' is required for httpRoutes.resolve"},
				{Path: "host", Type: "string", Message: "field 'host' is required for httpRoutes.resolve"},
				{Path: "path", Type: "string", Message: "field 'path' is required for httpRoutes.resolve"},
			}},
	}},
	{Domain: "dialogflow.googleapis.com", Methods: []MethodSchema{
		{HTTPMethod: "POST", PathGlob: "/v3/projects/*/locations/*/agents", ContentType: "application/json",
			RequiredBody: []BodyField{
				{Path: "displayName", Type: "string", Message: "field 'displayName' is required for agents.create"},
				{Path: "defaultLanguageCode", Type: "string", Message: "field 'defaultLanguageCode' is required for agents.create"},
				{Path: "timeZone", Type: "string", Message: "field 'timeZone' is required for agents.create"},
			}},
	}},
	{Domain: "privateca.googleapis.com", Methods: []MethodSchema{
		{HTTPMethod: "POST", PathGlob: "/v1/projects/*/locations/*/caPools/*/certificates", ContentType: "application/json",
			RequiredBody: []BodyField{
				{Path: "pemCsr", Type: "string", Message: "field 'pemCsr' is required for certificates.create"},
				{Path: "lifetime", Type: "string", Message: "field 'lifetime' is required for certificates.create"},
			}},
		{HTTPMethod: "POST", PathGlob: "/v1/projects/*/locations/*/caPools/*/certificates/*:revoke", ContentType: "application/json",
			RequiredBody: []BodyField{{Path: "reason", Type: "string", Message: "field 'reason' is required for certificates.revoke"}}},
	}},
	{Domain: "binaryauthorization.googleapis.com", Methods: []MethodSchema{
		{HTTPMethod: "PUT", PathGlob: "/v1/projects/*/policy", ContentType: "application/json",
			RequiredBody: []BodyField{
				{Path: "name", Type: "string", Message: "field 'name' is required for policy.update"},
				{Path: "defaultAdmissionRule", Type: "object", Message: "field 'defaultAdmissionRule' is required for policy.update"},
			}},
		{HTTPMethod: "POST", PathGlob: "/v1/projects/*/policy:evaluate", ContentType: "application/json",
			RequiredBody: []BodyField{{Path: "image", Type: "string", Message: "field 'image' is required for policy.evaluate"}}},
	}},
}
