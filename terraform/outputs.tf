output "bigquery_dataset_id" {
  description = "ID of the compatibility BigQuery dataset"
  value       = google_bigquery_dataset.compatibility.dataset_id
}

output "bigquery_table_id" {
  description = "ID of the compatibility BigQuery table"
  value       = google_bigquery_table.events.table_id
}

output "service_account_email" {
  description = "Email of the compatibility IAM service account"
  value       = google_service_account.compatibility.email
}
