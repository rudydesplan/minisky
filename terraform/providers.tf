locals {
  minisky_base_url = trimsuffix(var.minisky_endpoint, "/")
  use_minisky      = var.profile == "local"
}

provider "google" {
  project = var.project_id
  region  = var.region

  access_token = local.use_minisky ? var.local_access_token : null

  artifact_registry_custom_endpoint = local.use_minisky ? "${local.minisky_base_url}/_minisky/artifactregistry/" : null
  alloydb_custom_endpoint           = local.use_minisky ? "${local.minisky_base_url}/_minisky/alloydb/v1/" : null
  service_directory_custom_endpoint = local.use_minisky ? "${local.minisky_base_url}/_minisky/servicedirectory/v1/" : null
  document_ai_custom_endpoint       = local.use_minisky ? "${local.minisky_base_url}/_minisky/documentai/v1/" : null
  org_policy_custom_endpoint        = local.use_minisky ? "${local.minisky_base_url}/_minisky/orgpolicy/v2/" : null
  big_query_custom_endpoint         = local.use_minisky ? "${local.minisky_base_url}/_minisky/bigquery/bigquery/v2/" : null
  compute_custom_endpoint           = local.use_minisky ? "${local.minisky_base_url}/_minisky/compute/compute/v1/" : null
  iam_beta_custom_endpoint          = local.use_minisky ? "${local.minisky_base_url}/_minisky/iam/v1/" : null
  iam_credentials_custom_endpoint   = local.use_minisky ? "${local.minisky_base_url}/_minisky/iamcredentials/v1/" : null
  pubsub_custom_endpoint            = local.use_minisky ? "${local.minisky_base_url}/_minisky/pubsub/v1/" : null
  redis_custom_endpoint             = local.use_minisky ? "${local.minisky_base_url}/_minisky/redis/v1/" : null
  sql_custom_endpoint               = local.use_minisky ? "${local.minisky_base_url}/_minisky/sqladmin/sql/v1beta4/" : null
  spanner_custom_endpoint           = local.use_minisky ? "${local.minisky_base_url}/_minisky/spanner/v1/" : null
  storage_custom_endpoint           = local.use_minisky ? "${local.minisky_base_url}/_minisky/storage/storage/v1/" : null
  storage_transfer_custom_endpoint  = local.use_minisky ? "${local.minisky_base_url}/_minisky/storagetransfer/v1/" : null
  container_custom_endpoint         = local.use_minisky ? "${local.minisky_base_url}/_minisky/container/v1/" : null
  composer_custom_endpoint          = local.use_minisky ? "${local.minisky_base_url}/_minisky/composer/v1/" : null
  eventarc_custom_endpoint          = local.use_minisky ? "${local.minisky_base_url}/_minisky/eventarc/v1/" : null
  filestore_custom_endpoint         = local.use_minisky ? "${local.minisky_base_url}/_minisky/file/v1/" : null
  identity_platform_custom_endpoint = local.use_minisky ? "${local.minisky_base_url}/_minisky/identityplatform/v2/" : null
  managed_kafka_custom_endpoint     = local.use_minisky ? "${local.minisky_base_url}/_minisky/managedkafka/v1/" : null
  workflows_custom_endpoint         = local.use_minisky ? "${local.minisky_base_url}/_minisky/workflows/v1/" : null
}

provider "google" {
  alias   = "secondary"
  project = var.secondary_project_id
  region  = var.region

  access_token = local.use_minisky ? var.local_access_token : null

  big_query_custom_endpoint = local.use_minisky ? "${local.minisky_base_url}/_minisky/bigquery/bigquery/v2/" : null
  pubsub_custom_endpoint    = local.use_minisky ? "${local.minisky_base_url}/_minisky/pubsub/v1/" : null
}
