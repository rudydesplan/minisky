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
  location      = "US"
  force_destroy = true

  labels = {
    environment = "minisky"
    managed_by  = "terraform"
  }
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
  count = local.use_minisky ? 1 : 0

  name           = "minisky-terraform"
  tier           = "BASIC"
  memory_size_gb = 1
  region         = var.region
  redis_version  = "REDIS_7_2"
}

resource "google_spanner_instance" "compatibility" {
  count = local.use_minisky ? 1 : 0

  name          = "minisky-terraform"
  config        = "emulator-config"
  display_name  = "MiniSky Terraform compatibility"
  num_nodes     = 1
  force_destroy = true
}

resource "google_spanner_database" "compatibility" {
  count = local.use_minisky ? 1 : 0

  instance            = google_spanner_instance.compatibility[0].name
  name                = "minisky-terraform"
  deletion_protection = false
  ddl = [
    "CREATE TABLE Entries (Id STRING(64) NOT NULL, Value STRING(MAX)) PRIMARY KEY (Id)",
  ]
}
