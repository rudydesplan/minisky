variable "bigquery_location" {
  description = "BigQuery location used by the compatibility dataset"
  type        = string
  default     = "US"
}

variable "dataset_id" {
  description = "ID of the BigQuery dataset created by the example"
  type        = string
  default     = "minisky_terraform"
}

variable "enable_phase15_resources" {
  description = "Create the optional local Redis and Spanner Phase-15 resources"
  type        = bool
  default     = false
}

variable "enable_phase10_lb_resources" {
  description = "Create the optional local classic global HTTP load-balancer compatibility graph"
  type        = bool
  default     = false
}

variable "enable_phase10_artifact_resources" {
  description = "Create the optional local Artifact Registry repository compatibility resource"
  type        = bool
  default     = false
}

variable "local_access_token" {
  description = "Non-secret access token used only for the local MiniSky profile"
  type        = string
  default     = "minisky-local-token"
  sensitive   = true
}

variable "minisky_endpoint" {
  description = "Base URL of the MiniSky API gateway when profile is local"
  type        = string
  default     = "http://127.0.0.1:8080"
}

variable "profile" {
  description = "Provider target: local routes supported APIs through MiniSky; cloud uses normal Google endpoints and Application Default Credentials"
  type        = string
  default     = "local"

  validation {
    condition     = contains(["local", "cloud"], var.profile)
    error_message = "profile must be either local or cloud."
  }
}

variable "project_id" {
  description = "Google Cloud project ID used in resource names and API paths"
  type        = string
  default     = "local-dev-project"
}

variable "secondary_project_id" {
  description = "Second Google Cloud project used for isolation and cross-project acceptance"
  type        = string
  default     = "local-secondary-project"
}

variable "region" {
  description = "Default Google provider region"
  type        = string
  default     = "us-central1"
}

variable "service_account_id" {
  description = "Account ID of the metadata-only IAM service account"
  type        = string
  default     = "minisky-terraform"
}

variable "storage_bucket_name" {
  description = "Name of the Cloud Storage bucket created by the compatibility example"
  type        = string
  default     = "minisky-terraform-compatibility"
}

variable "table_id" {
  description = "ID of the BigQuery table created by the example"
  type        = string
  default     = "events"
}
