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

output "storage_bucket_name" {
  description = "Name of the compatibility Cloud Storage bucket"
  value       = google_storage_bucket.compatibility.name
}

output "secondary_bigquery_dataset_id" {
  description = "ID of the same-named dataset isolated in the second project"
  value       = google_bigquery_dataset.secondary_compatibility.dataset_id
}

output "cross_project_subscription" {
  description = "Subscription in the second project attached to the first-project topic"
  value       = google_pubsub_subscription.cross_project.id
}

output "phase15_redis_instance_name" {
  description = "Name of the optional local Phase-15 Redis instance, or null when disabled"
  value       = var.enable_phase15_resources ? google_redis_instance.compatibility[0].name : null
}

output "phase15_spanner_instance_name" {
  description = "Name of the optional local Phase-15 Spanner instance, or null when disabled"
  value       = var.enable_phase15_resources ? google_spanner_instance.compatibility[0].name : null
}

output "phase15_spanner_database_name" {
  description = "Name of the optional local Phase-15 Spanner database, or null when disabled"
  value       = var.enable_phase15_resources ? google_spanner_database.compatibility[0].name : null
}

output "phase10_artifact_repository_name" {
  description = "Name of the optional local Phase-10 Artifact Registry repository, or null when disabled"
  value       = var.enable_phase10_artifact_resources ? google_artifact_registry_repository.phase10[0].name : null
}

output "phase10_forwarding_rule_name" {
  description = "Name of the optional local Phase-10 global forwarding rule, or null when disabled"
  value       = var.enable_phase10_lb_resources ? google_compute_global_forwarding_rule.phase10_http[0].name : null
}

output "phase10_forwarding_proxy_url" {
  description = "MiniSky HTTP proxy URL for the optional local Phase-10 forwarding rule"
  value = var.enable_phase10_lb_resources ? format(
    "%s/_minisky/compute/compute/v1/projects/%s/global/forwardingRules/%s/proxy/",
    local.minisky_base_url,
    var.project_id,
    google_compute_global_forwarding_rule.phase10_http[0].name,
  ) : null
}
