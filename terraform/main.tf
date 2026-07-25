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
