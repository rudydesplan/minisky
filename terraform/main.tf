resource "google_bigquery_dataset" "compatibility" {
  dataset_id                 = var.dataset_id
  description                = "MiniSky Terraform compatibility dataset"
  location                   = var.bigquery_location
  delete_contents_on_destroy = true

  labels = {
    environment = "minisky"
    managed_by  = "terraform"
  }
}

resource "google_bigquery_table" "events" {
  dataset_id          = google_bigquery_dataset.compatibility.dataset_id
  table_id            = var.table_id
  deletion_protection = false
  description         = "Events managed by the MiniSky compatibility example"

  schema = jsonencode([
    {
      mode = "REQUIRED"
      name = "event_id"
      type = "STRING"
    },
    {
      mode = "NULLABLE"
      name = "created_at"
      type = "TIMESTAMP"
    },
  ])
}

resource "google_service_account" "compatibility" {
  account_id   = var.service_account_id
  display_name = "MiniSky Terraform compatibility"
  description  = "Metadata-only service account used by the compatibility stack"
}

resource "google_storage_bucket" "compatibility" {
  name          = var.storage_bucket_name
  location      = "US-CENTRAL1"
  force_destroy = true
}

resource "google_bigquery_dataset" "secondary_compatibility" {
  provider = google.secondary

  dataset_id                 = var.dataset_id
  description                = "MiniSky second-project isolation dataset"
  location                   = var.bigquery_location
  delete_contents_on_destroy = true
}

resource "google_pubsub_topic" "cross_project" {
  name    = "minisky-cross-project"
  project = var.project_id
}

resource "google_pubsub_subscription" "cross_project" {
  provider = google.secondary

  name    = "minisky-cross-project"
  project = var.secondary_project_id
  topic   = google_pubsub_topic.cross_project.id
}

# These emulator-backed resources are intentionally local-profile-only. The
# Spanner emulator exposes its exact admin/LRO contract, while MiniSky owns the
# Redis control plane and loopback Docker data plane.
resource "google_redis_instance" "compatibility" {
  count = local.use_minisky && var.enable_phase15_resources ? 1 : 0

  name           = "minisky-terraform"
  tier           = "BASIC"
  memory_size_gb = 1
  region         = var.region
  redis_version  = "REDIS_7_2"
}

resource "google_spanner_instance" "compatibility" {
  count = local.use_minisky && var.enable_phase15_resources ? 1 : 0

  name          = "minisky-terraform"
  config        = "emulator-config"
  display_name  = "MiniSky Terraform compatibility"
  num_nodes     = 1
  force_destroy = true
}

resource "google_spanner_database" "compatibility" {
  count = local.use_minisky && var.enable_phase15_resources ? 1 : 0

  instance            = google_spanner_instance.compatibility[0].name
  name                = "minisky-terraform"
  deletion_protection = false
  ddl = [
    "CREATE TABLE Entries (Id STRING(64) NOT NULL, Value STRING(MAX)) PRIMARY KEY (Id)",
  ]
}

# These resources are off by default because they exercise real profile-owned
# Docker/Kind backends. The guarded integration runner enables them explicitly.
resource "google_sql_database_instance" "fidelity" {
  count = local.use_minisky && var.enable_fidelity_cloudsql_resources ? 1 : 0

  name             = "minisky-fidelity"
  database_version = "POSTGRES_15"
  region           = var.region

  settings {
    tier = "db-f1-micro"
  }

  deletion_protection = false
}

resource "google_sql_database" "fidelity" {
  count = local.use_minisky && var.enable_fidelity_cloudsql_resources ? 1 : 0

  name     = "app"
  instance = google_sql_database_instance.fidelity[0].name
}

resource "google_sql_user" "fidelity" {
  count = local.use_minisky && var.enable_fidelity_cloudsql_resources ? 1 : 0

  name     = "app_user"
  instance = google_sql_database_instance.fidelity[0].name
  password = "minisky-local-only"
}

resource "google_container_cluster" "fidelity" {
  count = local.use_minisky && var.enable_fidelity_gke_resources ? 1 : 0

  name               = "minisky-fidelity"
  location           = "us-central1-c"
  initial_node_count = 1

  deletion_protection = false
}

# This opt-in local slice exercises one custom-mode VPC and its single bounded
# regional IPv4 subnetwork. NAT, peering, PSC, IPv6, and secondary ranges remain
# explicit non-goals for this compatibility fixture.
resource "google_compute_network" "phase16" {
  count = local.use_minisky && var.enable_phase16_network_resources ? 1 : 0

  name                    = var.phase16_network_name
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "phase16" {
  count = local.use_minisky && var.enable_phase16_network_resources ? 1 : 0

  name          = var.phase16_subnetwork_name
  ip_cidr_range = var.phase16_subnetwork_cidr
  network       = google_compute_network.phase16[0].id
  region        = var.region
}

# This instance intentionally exercises only one NIC on the bounded regional
# IPv4 subnetwork. It has no access_config, IPv6, additional NIC, or implicit
# NAT claim.
resource "google_compute_instance" "phase16" {
  count = local.use_minisky && var.enable_phase16_network_resources && var.enable_phase16_compute_instance ? 1 : 0

  name         = var.phase16_instance_name
  machine_type = "e2-micro"
  zone         = var.phase16_instance_zone

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-12"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.phase16[0].id
  }
}

# This first Phase-18 provider slice is deliberately limited to persisted
# Workflows metadata. Executions and Eventarc delivery remain generated-client
# evidence, not Terraform-managed resources.
resource "google_workflows_workflow" "phase18" {
  count = local.use_minisky && var.enable_phase18_workflows_resource ? 1 : 0

  name                = var.phase18_workflow_name
  region              = var.region
  description         = "MiniSky Phase-18 Terraform lifecycle evidence"
  source_contents     = <<-YAML
    - return_result:
        return: "minisky-phase18"
  YAML
  deletion_protection = false

  timeouts {
    create = "2m"
    update = "2m"
    delete = "2m"
  }
}

# This dependent Phase-18 provider slice exercises Eventarc trigger
# control-plane metadata only: one event-type filter, a persisted Workflows
# destination, and explicit Pub/Sub transport metadata. The gate does not
# publish events or claim that MiniSky provisions a real Eventarc transport.
resource "google_eventarc_trigger" "phase18" {
  count = local.use_minisky && var.enable_phase18_eventarc_resource && var.enable_phase18_workflows_resource ? 1 : 0

  name     = var.phase18_eventarc_trigger_name
  location = var.region

  event_data_content_type = "application/json"
  labels = {
    goog-terraform-provisioned = "true"
  }

  matching_criteria {
    attribute = "type"
    value     = "google.cloud.storage.object.v1.finalized"
  }

  destination {
    workflow = google_workflows_workflow.phase18[0].id
  }

  transport {
    pubsub {
      topic = "projects/${var.project_id}/topics/${var.phase18_eventarc_transport_topic}"
    }
  }

  timeouts {
    create = "2m"
    update = "2m"
    delete = "2m"
  }
}

# This heavy opt-in fixture claims only exact-owned local Airflow backend
# lifecycle plus persisted Composer environment metadata.
resource "google_composer_environment" "phase19" {
  count = local.use_minisky && var.enable_phase19_composer_resource ? 1 : 0

  name   = var.phase19_composer_environment_name
  region = var.region

  labels = {
    goog-terraform-provisioned = "true"
  }

  timeouts {
    create = "5m"
    update = "2m"
    delete = "2m"
  }
}

# This heavy opt-in fixture treats capacity and subnet as persisted control-plane
# metadata only. The executable local broker is exact-owned, loopback plaintext
# Kafka; MiniSky does not emulate GCP VPC attachment or managed TLS.
resource "google_managed_kafka_cluster" "phase19" {
  count = local.use_minisky && var.enable_phase19_managed_kafka_resource ? 1 : 0

  cluster_id = var.phase19_managed_kafka_cluster_id
  location   = var.region

  capacity_config {
    vcpu_count   = 3
    memory_bytes = 3221225472
  }

  gcp_config {
    access_config {
      network_configs {
        subnet = "projects/${var.project_id}/regions/${var.region}/subnetworks/minisky-metadata-only"
      }
    }
  }

  labels = {
    goog-terraform-provisioned = "true"
  }

  timeouts {
    create = "5m"
    update = "2m"
    delete = "2m"
  }
}

# This opt-in fixture maps the provider's mandatory share onto MiniSky's
# traversal-protected profile filesystem. Its network is opaque metadata only:
# no NFS server, mount protocol, VPC attachment, or address allocation is claimed.
resource "google_filestore_instance" "phase20" {
  count = local.use_minisky && var.enable_phase20_filestore_resource ? 1 : 0

  name     = var.phase20_filestore_instance_name
  location = "${var.region}-a"
  tier     = "BASIC_HDD"

  file_shares {
    capacity_gb = 1024
    name        = "minisky"
  }

  networks {
    network = "minisky-metadata-only"
    modes   = ["MODE_IPV4"]
  }

  labels = {
    goog-terraform-provisioned = "true"
  }

  timeouts {
    create = "2m"
    update = "2m"
    delete = "2m"
  }
}

# Project config is a singleton that the provider cannot delete. This bounded
# fixture exercises authorized-domain metadata only; the gate explicitly resets
# it before removing Terraform state.
resource "google_identity_platform_config" "phase20" {
  count = local.use_minisky && var.enable_phase20_identity_platform_config ? 1 : 0

  authorized_domains = var.phase20_identity_platform_authorized_domains
}

# This bounded job copies only between isolated local Storage-emulator buckets.
# Cloud credentials, external sources, agents, scheduling, and cloud networking
# are outside this fixture.
resource "google_storage_transfer_job" "phase20" {
  count = local.use_minisky && var.enable_phase20_storage_transfer_job ? 1 : 0

  description = "MiniSky bounded local GCS-to-GCS transfer"
  project     = var.project_id
  status      = "ENABLED"

  transfer_spec {
    gcs_data_source {
      bucket_name = var.phase20_storage_transfer_source_bucket
    }
    gcs_data_sink {
      bucket_name = var.phase20_storage_transfer_sink_bucket
    }
  }
}

resource "google_alloydb_cluster" "phase20" {
  count = local.use_minisky && var.enable_phase20_alloydb_resources ? 1 : 0

  cluster_id          = var.phase20_alloydb_cluster_id
  location            = var.region
  deletion_protection = false

  network_config {
    network = "projects/${var.project_id}/global/networks/minisky-metadata-only"
  }
}

resource "google_alloydb_instance" "phase20" {
  count = local.use_minisky && var.enable_phase20_alloydb_resources ? 1 : 0

  cluster       = google_alloydb_cluster.phase20[0].name
  instance_id   = var.phase20_alloydb_instance_id
  instance_type = "PRIMARY"
}

resource "google_service_directory_namespace" "phase21" {
  count = local.use_minisky && var.enable_phase21_service_directory_resources ? 1 : 0

  namespace_id = var.phase21_service_directory_namespace_id
  location     = var.region
  labels = {
    purpose = "metadata-only"
  }
}

resource "google_service_directory_service" "phase21" {
  count = local.use_minisky && var.enable_phase21_service_directory_resources ? 1 : 0

  service_id = var.phase21_service_directory_service_id
  namespace  = google_service_directory_namespace.phase21[0].name
  metadata = {
    protocol = "opaque"
  }
}

resource "google_service_directory_endpoint" "phase21" {
  count = local.use_minisky && var.enable_phase21_service_directory_resources ? 1 : 0

  endpoint_id = var.phase21_service_directory_endpoint_id
  service     = google_service_directory_service.phase21[0].name
  address     = "127.0.0.1"
  port        = 8080
  metadata = {
    resolution = "unsupported"
  }
}

resource "google_document_ai_processor" "phase23" {
  count = local.use_minisky && var.enable_phase23_document_ai_processor ? 1 : 0

  location     = var.region
  display_name = var.phase23_document_ai_processor_display_name
  type         = "OCR_PROCESSOR"
}

resource "google_org_policy_policy" "phase24" {
  count = local.use_minisky && var.enable_phase24_org_policy ? 1 : 0

  name   = "projects/${var.project_id}/policies/compute.disableSerialPortAccess"
  parent = "projects/${var.project_id}"

  spec {
    rules {
      enforce = "TRUE"
    }
  }
}

# The repository metadata is served by MiniSky while pushed image manifests and
# blobs live in the profile-owned Registry v2 backend.
resource "google_artifact_registry_repository" "phase10" {
  count = local.use_minisky && var.enable_phase10_artifact_resources ? 1 : 0

  location      = var.region
  repository_id = "minisky-phase10"
  description   = "MiniSky Phase-10 Artifact Registry compatibility"
  format        = "DOCKER"

  labels = {
    environment = "minisky"
    managed_by  = "terraform"
  }
}

# This local-only graph intentionally exercises the bounded classic global HTTP
# load-balancer slice: one unmanaged zonal group and defaultService routing.
resource "google_compute_firewall" "phase10_http" {
  count = local.use_minisky && var.enable_phase10_lb_resources ? 1 : 0

  name    = "minisky-phase10-http"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["80"]
  }

  source_ranges = ["127.0.0.1/32"]
}

resource "google_compute_instance" "phase10_http" {
  count = local.use_minisky && var.enable_phase10_lb_resources ? 1 : 0

  name         = "minisky-phase10-http"
  machine_type = "e2-micro"
  zone         = "us-central1-a"

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-12"
    }
  }

  network_interface {
    network = "default"
  }

  depends_on = [google_compute_firewall.phase10_http]
}

resource "google_compute_instance_group" "phase10_http" {
  count = local.use_minisky && var.enable_phase10_lb_resources ? 1 : 0

  name      = "minisky-phase10-http"
  zone      = "us-central1-a"
  instances = [google_compute_instance.phase10_http[0].self_link]

  named_port {
    name = "http"
    port = 80
  }
}

resource "google_compute_health_check" "phase10_http" {
  count = local.use_minisky && var.enable_phase10_lb_resources ? 1 : 0

  name = "minisky-phase10-http"

  http_health_check {
    port         = 80
    request_path = "/"
  }
}

resource "google_compute_backend_service" "phase10_http" {
  count = local.use_minisky && var.enable_phase10_lb_resources ? 1 : 0

  name          = "minisky-phase10-http"
  protocol      = "HTTP"
  port_name     = "http"
  health_checks = [google_compute_health_check.phase10_http[0].self_link]

  backend {
    group = google_compute_instance_group.phase10_http[0].self_link
  }
}

resource "google_compute_url_map" "phase10_http" {
  count = local.use_minisky && var.enable_phase10_lb_resources ? 1 : 0

  name            = "minisky-phase10-http"
  default_service = google_compute_backend_service.phase10_http[0].self_link
}

resource "google_compute_target_http_proxy" "phase10_http" {
  count = local.use_minisky && var.enable_phase10_lb_resources ? 1 : 0

  name    = "minisky-phase10-http"
  url_map = google_compute_url_map.phase10_http[0].self_link
}

resource "google_compute_global_forwarding_rule" "phase10_http" {
  count = local.use_minisky && var.enable_phase10_lb_resources ? 1 : 0

  name                  = "minisky-phase10-http"
  target                = google_compute_target_http_proxy.phase10_http[0].self_link
  port_range            = "80"
  ip_protocol           = "TCP"
  load_balancing_scheme = "EXTERNAL"
}

locals {
  phase13_wif_enabled = local.use_minisky && var.enable_phase13_wif_resources
  phase13_wif_delegates = local.phase13_wif_enabled ? {
    for index, account_id in var.phase13_wif_delegate_account_ids : tostring(index) => account_id
  } : {}
  phase13_wif_delegation_edges = local.phase13_wif_enabled ? {
    for index, account_id in var.phase13_wif_delegate_account_ids : tostring(index) => {
      source_key = tostring(index)
      target_key = index + 1 < length(var.phase13_wif_delegate_account_ids) ? tostring(index + 1) : null
    }
  } : {}
}

# Phase-13 exercises only the local, bounded static-JWKS OIDC path. The private
# signing key and signed subject JWT are generated by the guarded integration
# script and never enter Terraform configuration or state.
resource "google_iam_workload_identity_pool" "phase13" {
  count = local.phase13_wif_enabled ? 1 : 0

  workload_identity_pool_id = "minisky-phase13"
  display_name              = "MiniSky Phase-13"
  description               = "Local workload identity federation compatibility pool"
}

resource "google_iam_workload_identity_pool_provider" "phase13" {
  count = local.phase13_wif_enabled ? 1 : 0

  workload_identity_pool_id          = google_iam_workload_identity_pool.phase13[0].workload_identity_pool_id
  workload_identity_pool_provider_id = "minisky-oidc"
  display_name                       = "MiniSky static JWKS"
  description                        = "Local inline-JWKS OIDC provider"
  attribute_mapping = {
    "google.subject" = "assertion.sub"
  }

  oidc {
    issuer_uri        = var.phase13_wif_issuer_uri
    allowed_audiences = [var.phase13_wif_token_audience]
    jwks_json         = var.phase13_wif_public_jwks
  }
}

resource "google_service_account" "phase13_delegate" {
  for_each = local.phase13_wif_delegates

  account_id   = each.value
  display_name = "MiniSky Phase-13 delegate ${tonumber(each.key) + 1}"
  description  = "Local workload identity delegation chain member"
}

resource "google_service_account" "phase13_target" {
  count = local.phase13_wif_enabled ? 1 : 0

  account_id   = "minisky-wif-target"
  display_name = "MiniSky Phase-13 final target"
  description  = "Final service account impersonated by the local WIF chain"
}

resource "google_service_account_iam_member" "phase13_federated_caller" {
  count = local.phase13_wif_enabled ? 1 : 0

  service_account_id = google_service_account.phase13_delegate["0"].name
  role               = "roles/iam.workloadIdentityUser"
  member = format(
    "principal://iam.googleapis.com/%s/subject/%s",
    google_iam_workload_identity_pool.phase13[0].name,
    var.phase13_wif_subject,
  )
}

resource "google_service_account_iam_member" "phase13_delegation" {
  for_each = local.phase13_wif_delegation_edges

  service_account_id = each.value.target_key == null ? google_service_account.phase13_target[0].name : google_service_account.phase13_delegate[each.value.target_key].name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = google_service_account.phase13_delegate[each.value.source_key].member
}

resource "google_service_account_iam_member" "phase13_target_reader" {
  count = local.phase13_wif_enabled ? 1 : 0

  service_account_id = google_service_account.phase13_target[0].name
  role               = "roles/iam.serviceAccountViewer"
  member             = google_service_account.phase13_target[0].member
}
