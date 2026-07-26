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

output "phase16_network_id" {
  description = "Canonical ID of the optional local Phase-16 custom network, or null when disabled"
  value       = local.use_minisky && var.enable_phase16_network_resources ? google_compute_network.phase16[0].id : null
}

output "phase16_subnetwork_id" {
  description = "Canonical ID of the optional local Phase-16 regional subnetwork, or null when disabled"
  value       = local.use_minisky && var.enable_phase16_network_resources ? google_compute_subnetwork.phase16[0].id : null
}

output "phase16_instance_id" {
  description = "Canonical ID of the optional local Phase-16 Compute instance, or null when disabled"
  value       = local.use_minisky && var.enable_phase16_network_resources && var.enable_phase16_compute_instance ? google_compute_instance.phase16[0].id : null
}

output "phase16_instance_network_ip" {
  description = "Docker-observed primary IPv4 address of the optional local Phase-16 instance"
  value       = local.use_minisky && var.enable_phase16_network_resources && var.enable_phase16_compute_instance ? google_compute_instance.phase16[0].network_interface[0].network_ip : null
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

output "phase13_wif_pool_name" {
  description = "Canonical name of the optional local Phase-13 workload identity pool, or null when disabled"
  value       = local.phase13_wif_enabled ? google_iam_workload_identity_pool.phase13[0].name : null
}

output "phase13_wif_provider_name" {
  description = "Canonical name of the optional local Phase-13 workload identity provider, or null when disabled"
  value       = local.phase13_wif_enabled ? google_iam_workload_identity_pool_provider.phase13[0].name : null
}

output "phase13_wif_sts_audience" {
  description = "Canonical STS exchange audience for the optional local Phase-13 provider, or null when disabled"
  value       = local.phase13_wif_enabled ? "//iam.googleapis.com/${google_iam_workload_identity_pool_provider.phase13[0].name}" : null
}

output "phase13_wif_delegate_emails" {
  description = "Ordered delegate service account emails for the optional local Phase-13 chain"
  value = local.phase13_wif_enabled ? [
    for index in range(length(var.phase13_wif_delegate_account_ids)) :
    google_service_account.phase13_delegate[tostring(index)].email
  ] : []
}

output "phase13_wif_target_email" {
  description = "Final target service account email for the optional local Phase-13 chain, or null when disabled"
  value       = local.phase13_wif_enabled ? google_service_account.phase13_target[0].email : null
}
