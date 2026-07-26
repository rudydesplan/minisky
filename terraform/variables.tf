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

variable "enable_phase13_wif_resources" {
  description = "Create the optional local Phase-13 workload identity federation resources"
  type        = bool
  default     = false
}

variable "enable_phase16_network_resources" {
  description = "Create the optional local Phase-16 custom network and bounded IPv4 subnetwork"
  type        = bool
  default     = false
}

variable "phase16_network_name" {
  description = "Name of the optional local Phase-16 custom-mode network"
  type        = string
  default     = "minisky-phase16"

  validation {
    condition     = can(regex("^[a-z](?:[-a-z0-9]{0,61}[a-z0-9])?$", var.phase16_network_name))
    error_message = "phase16_network_name must follow GCE RFC1035 naming rules."
  }
}

variable "phase16_subnetwork_cidr" {
  description = "Canonical IPv4 CIDR assigned to the optional local Phase-16 subnetwork"
  type        = string
  default     = "10.42.0.0/24"

  validation {
    condition = try(
      cidrnetmask(var.phase16_subnetwork_cidr) != "" &&
      cidrhost(var.phase16_subnetwork_cidr, 0) == split("/", var.phase16_subnetwork_cidr)[0] &&
      tonumber(split("/", var.phase16_subnetwork_cidr)[1]) >= 8 &&
      tonumber(split("/", var.phase16_subnetwork_cidr)[1]) <= 29 &&
      can(regex("^(10\\.|172\\.(1[6-9]|2[0-9]|3[01])\\.|192\\.168\\.)", var.phase16_subnetwork_cidr)),
      false,
    )
    error_message = "phase16_subnetwork_cidr must be a canonical private IPv4 /8 through /29 prefix."
  }
}

variable "phase16_subnetwork_name" {
  description = "Name of the optional local Phase-16 regional subnetwork"
  type        = string
  default     = "minisky-phase16"

  validation {
    condition     = can(regex("^[a-z](?:[-a-z0-9]{0,61}[a-z0-9])?$", var.phase16_subnetwork_name))
    error_message = "phase16_subnetwork_name must follow GCE RFC1035 naming rules."
  }
}

variable "phase13_wif_delegate_account_ids" {
  description = "Ordered account IDs for the local Phase-13 delegation chain"
  type        = list(string)
  default     = ["minisky-wif-delegate"]

  validation {
    condition = (
      length(var.phase13_wif_delegate_account_ids) >= 1 &&
      length(var.phase13_wif_delegate_account_ids) <= 4 &&
      length(distinct(var.phase13_wif_delegate_account_ids)) == length(var.phase13_wif_delegate_account_ids) &&
      alltrue([
        for account_id in var.phase13_wif_delegate_account_ids :
        can(regex("^[a-z][a-z0-9-]{4,28}[a-z0-9]$", account_id))
      ])
    )
    error_message = "phase13_wif_delegate_account_ids must contain one to four unique service account IDs."
  }
}

variable "phase13_wif_issuer_uri" {
  description = "Issuer URI embedded in Phase-13 workload identity subject tokens"
  type        = string
  default     = "https://issuer.minisky.invalid"
}

variable "phase13_wif_public_jwks" {
  description = "Public static RSA JWKS used by the optional local Phase-13 OIDC provider"
  type        = string
  default     = null
  nullable    = true

  validation {
    condition     = !var.enable_phase13_wif_resources || var.profile != "local" || var.phase13_wif_public_jwks != null
    error_message = "phase13_wif_public_jwks is required when local Phase-13 WIF resources are enabled."
  }
}

variable "phase13_wif_subject" {
  description = "OIDC subject authorized to impersonate the first Phase-13 delegate"
  type        = string
  default     = "minisky-phase13-caller"

  validation {
    condition     = can(regex("^[A-Za-z0-9._~-]{1,128}$", var.phase13_wif_subject))
    error_message = "phase13_wif_subject must be a non-empty URL-path-safe subject."
  }
}

variable "phase13_wif_token_audience" {
  description = "Audience claim accepted in Phase-13 OIDC subject tokens"
  type        = string
  default     = "minisky-phase13"
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
