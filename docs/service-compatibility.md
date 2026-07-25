# Service compatibility

This matrix is the human-readable view of the executable manifest in
`pkg/registry`. The contract gate derives its service list from runtime
registrations, so adding or removing a registered domain requires updating the
manifest and this document.

Fidelity tiers:

- **high**: broad protocol and behavior compatibility for the documented API.
- **standard**: selected resource operations with GCP-shaped responses.
- **passthrough**: requests are delegated to a Docker-backed emulator.

Persistence categories describe where service state lives: **memory**, **file**,
**docker**, **hybrid** (control-plane metadata plus a data-plane backend), or
**static**.

| Domain | Fidelity | Persistence |
| --- | --- | --- |
| `aiplatform.googleapis.com` | standard | memory |
| `appengine.googleapis.com` | standard | hybrid |
| `artifactregistry.googleapis.com` | standard | memory |
| `bigquery.googleapis.com` | standard | file |
| `bigtable.googleapis.com` | standard | hybrid |
| `bigtableadmin.googleapis.com` | standard | hybrid |
| `cloudbuild.googleapis.com` | standard | hybrid |
| `cloudfunctions.googleapis.com` | standard | hybrid |
| `cloudkms.googleapis.com` | standard | file |
| `cloudscheduler.googleapis.com` | standard | file |
| `cloudtasks.googleapis.com` | standard | memory |
| `compute.googleapis.com` | standard | hybrid |
| `container.googleapis.com` | standard | file |
| `dataproc.googleapis.com` | standard | hybrid |
| `datastore.googleapis.com` | passthrough | docker |
| `dns.googleapis.com` | standard | file |
| `firebasehosting.googleapis.com` | passthrough | docker |
| `firebaseio.com` | passthrough | docker |
| `firestore.googleapis.com` | passthrough | docker |
| `iam.googleapis.com` | standard | file |
| `identitytoolkit.googleapis.com` | passthrough | docker |
| `logging.googleapis.com` | standard | file |
| `memcache.googleapis.com` | standard | hybrid |
| `metadata.google.internal` | high | static |
| `monitoring.googleapis.com` | standard | memory |
| `pubsub.googleapis.com` | passthrough | docker |
| `redis.googleapis.com` | standard | hybrid |
| `run.googleapis.com` | standard | hybrid |
| `secretmanager.googleapis.com` | standard | file |
| `spanner.googleapis.com` | passthrough | docker |
| `sqladmin.googleapis.com` | standard | hybrid |
| `storage.googleapis.com` | passthrough | docker |

The unsupported-route contract uses
`/__minisky_contract__/unsupported`. Probe-safe registered handlers must return
HTTP 501 with a GCP JSON error envelope and `UNIMPLEMENTED` status. Services
whose request path necessarily starts a lazy Docker backend are skipped through
explicit manifest metadata; they remain covered by registration and
documentation checks.

## Evidence boundaries

The manifest is a registration and unsupported-route contract, not a claim of
complete method parity. BigQuery has the deepest native conformance coverage.
The tracked Terraform and SDK smoke suites currently exercise BigQuery
dataset/table resources and IAM service accounts; broader service compatibility
must be supported by focused executable tests before its manifest fidelity is
raised.

See [State model](state-model.md) for restart and export behavior and
[Terraform compatibility](terraform-compatibility.md) for tested provider and
SDK coverage.
