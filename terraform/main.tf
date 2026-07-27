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
