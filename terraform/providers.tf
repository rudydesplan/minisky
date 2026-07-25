locals {
  minisky_base_url = trimsuffix(var.minisky_endpoint, "/")
  use_minisky      = var.profile == "local"
}

provider "google" {
  project = var.project_id
  region  = var.region

  access_token = local.use_minisky ? var.local_access_token : null

  big_query_custom_endpoint = local.use_minisky ? "${local.minisky_base_url}/_minisky/bigquery/bigquery/v2/" : null
  iam_beta_custom_endpoint  = local.use_minisky ? "${local.minisky_base_url}/_minisky/iam/v1/" : null
  pubsub_custom_endpoint    = local.use_minisky ? "${local.minisky_base_url}/_minisky/pubsub/v1/" : null
  redis_custom_endpoint     = local.use_minisky ? "${local.minisky_base_url}/_minisky/redis/v1/" : null
  spanner_custom_endpoint   = local.use_minisky ? "${local.minisky_base_url}/_minisky/spanner/v1/" : null
  storage_custom_endpoint   = local.use_minisky ? "${local.minisky_base_url}/_minisky/storage/storage/v1/" : null
}

provider "google" {
  alias   = "secondary"
  project = var.secondary_project_id
  region  = var.region

  access_token = local.use_minisky ? var.local_access_token : null

  big_query_custom_endpoint = local.use_minisky ? "${local.minisky_base_url}/_minisky/bigquery/bigquery/v2/" : null
  pubsub_custom_endpoint    = local.use_minisky ? "${local.minisky_base_url}/_minisky/pubsub/v1/" : null
}
