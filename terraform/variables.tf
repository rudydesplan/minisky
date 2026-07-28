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

variable "enable_memcache_resource" {
  description = "Create the guarded local Memorystore for Memcached compatibility instance"
  type        = bool
  default     = false
}

variable "memcache_instance_name" {
  description = "Name of the guarded local Memorystore for Memcached instance"
  type        = string
  default     = "minisky-memcached"

  validation {
    condition     = can(regex("^[a-z](?:[-a-z0-9]{0,38}[a-z0-9])?$", var.memcache_instance_name))
    error_message = "memcache_instance_name must start with a letter, contain only lowercase letters, digits, and hyphens, and be at most 40 characters."
  }
}

variable "enable_fidelity_cloudsql_resources" {
  description = "Create the guarded local Cloud SQL instance, database, and user lifecycle gate"
  type        = bool
  default     = false
}

variable "enable_fidelity_gke_resources" {
  description = "Create the guarded local GKE cluster lifecycle gate"
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

variable "enable_phase16_compute_instance" {
  description = "Create the optional local Phase-16 Compute instance on the bounded subnetwork"
  type        = bool
  default     = false
}

variable "enable_phase18_eventarc_resource" {
  description = "Create the optional local Phase-18 Eventarc trigger provider lifecycle resource"
  type        = bool
  default     = false
}

variable "enable_phase18_workflows_resource" {
  description = "Create the optional local Phase-18 Workflows provider lifecycle resource"
  type        = bool
  default     = false
}

variable "phase18_eventarc_transport_topic" {
  description = "Pub/Sub transport topic name persisted as metadata by the optional local Eventarc trigger"
  type        = string
  default     = "minisky-phase18-eventarc-transport"

  validation {
    condition     = can(regex("^[a-z]([-a-z0-9]{1,253}[a-z0-9])?$", var.phase18_eventarc_transport_topic))
    error_message = "phase18_eventarc_transport_topic must start with a letter, contain only lowercase letters, digits, and hyphens, and be between 3 and 255 characters."
  }
}

variable "phase18_eventarc_trigger_name" {
  description = "Name of the optional local Phase-18 Eventarc trigger lifecycle resource"
  type        = string
  default     = "minisky-phase18-eventarc"

  validation {
    condition     = can(regex("^[a-z]([-a-z0-9]{0,61}[a-z0-9])?$", var.phase18_eventarc_trigger_name))
    error_message = "phase18_eventarc_trigger_name must start with a letter, contain only lowercase letters, digits, and hyphens, and be at most 63 characters."
  }
}

variable "phase18_workflow_name" {
  description = "Name of the optional local Phase-18 Workflows lifecycle resource"
  type        = string
  default     = "minisky-phase18"

  validation {
    condition     = can(regex("^[a-z]([-a-z0-9]{0,61}[a-z0-9])?$", var.phase18_workflow_name))
    error_message = "phase18_workflow_name must start with a letter, contain only lowercase letters, digits, and hyphens, and be at most 63 characters."
  }
}

variable "enable_phase19_composer_resource" {
  description = "Create the optional heavy local Phase-19 Composer provider lifecycle resource"
  type        = bool
  default     = false
}

variable "phase19_composer_environment_name" {
  description = "Name of the optional heavy local Phase-19 Composer environment"
  type        = string
  default     = "minisky-phase19-composer"

  validation {
    condition     = can(regex("^[a-z]([-a-z0-9]{0,61}[a-z0-9])?$", var.phase19_composer_environment_name))
    error_message = "phase19_composer_environment_name must start with a letter, contain only lowercase letters, digits, and hyphens, and be at most 63 characters."
  }
}

variable "enable_phase19_managed_kafka_resource" {
  description = "Create the optional heavy local Phase-19 Managed Kafka provider lifecycle resource"
  type        = bool
  default     = false
}

variable "phase19_managed_kafka_cluster_id" {
  description = "ID of the optional heavy local Phase-19 Managed Kafka cluster"
  type        = string
  default     = "minisky-phase19-kafka"

  validation {
    condition     = can(regex("^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$", var.phase19_managed_kafka_cluster_id))
    error_message = "phase19_managed_kafka_cluster_id must start with a letter, contain only lowercase letters, digits, and hyphens, and be at most 63 characters."
  }
}

variable "enable_phase20_filestore_resource" {
  description = "Create the optional local Phase-20 Filestore provider lifecycle resource"
  type        = bool
  default     = false
}

variable "phase20_filestore_instance_name" {
  description = "Name of the optional local Phase-20 Filestore instance"
  type        = string
  default     = "minisky-phase20-filestore"

  validation {
    condition     = can(regex("^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$", var.phase20_filestore_instance_name))
    error_message = "phase20_filestore_instance_name must start with a letter, contain only lowercase letters, digits, and hyphens, and be at most 63 characters."
  }
}

variable "enable_phase20_identity_platform_config" {
  description = "Manage the optional local Phase-20 Identity Platform project config"
  type        = bool
  default     = false
}

variable "phase20_identity_platform_authorized_domains" {
  description = "Authorized-domain metadata for the optional local Identity Platform project config"
  type        = list(string)
  default     = ["localhost"]
}

variable "enable_phase20_storage_transfer_job" {
  description = "Create the optional local Phase-20 Storage Transfer job"
  type        = bool
  default     = false
}

variable "phase20_storage_transfer_source_bucket" {
  description = "Source bucket for the optional local Storage Transfer job"
  type        = string
  default     = "minisky-phase20-transfer-source"
}

variable "phase20_storage_transfer_sink_bucket" {
  description = "Sink bucket for the optional local Storage Transfer job"
  type        = string
  default     = "minisky-phase20-transfer-sink"
}

variable "enable_phase20_alloydb_resources" {
  description = "Create the optional heavy local Phase-20 AlloyDB cluster and primary instance"
  type        = bool
  default     = false
}

variable "phase20_alloydb_cluster_id" {
  description = "Cluster ID for the optional local AlloyDB lifecycle"
  type        = string
  default     = "minisky-phase20-alloydb"
}

variable "phase20_alloydb_instance_id" {
  description = "Primary instance ID for the optional local AlloyDB lifecycle"
  type        = string
  default     = "minisky-primary"
}

variable "enable_phase21_service_directory_resources" {
  description = "Create the optional Phase-21 Service Directory namespace/service/endpoint hierarchy"
  type        = bool
  default     = false
}

variable "phase21_service_directory_namespace_id" {
  description = "Namespace ID for the optional Service Directory lifecycle"
  type        = string
  default     = "minisky-phase21"
}

variable "phase21_service_directory_service_id" {
  description = "Service ID for the optional Service Directory lifecycle"
  type        = string
  default     = "local-service"
}

variable "phase21_service_directory_endpoint_id" {
  description = "Endpoint ID for the optional Service Directory lifecycle"
  type        = string
  default     = "local-endpoint"
}

variable "enable_phase23_document_ai_processor" {
  description = "Create the optional Phase-23 metadata-only Document AI processor"
  type        = bool
  default     = false
}

variable "phase23_document_ai_processor_display_name" {
  description = "Display name for the optional local Document AI processor"
  type        = string
  default     = "minisky-phase23-processor"
}

variable "enable_phase24_org_policy" {
  description = "Create the optional Phase-24 project Organization Policy simulation"
  type        = bool
  default     = false
}

variable "enable_phase25_binary_authorization_policy" {
  description = "Manage the optional local Phase-25 Binary Authorization project policy"
  type        = bool
  default     = false
}

variable "phase16_instance_name" {
  description = "Name of the optional local Phase-16 Compute instance"
  type        = string
  default     = "minisky-phase16"

  validation {
    condition     = can(regex("^[a-z](?:[-a-z0-9]{0,61}[a-z0-9])?$", var.phase16_instance_name))
    error_message = "phase16_instance_name must follow GCE RFC1035 naming rules."
  }
}

variable "phase16_instance_zone" {
  description = "Zone of the optional local Phase-16 Compute instance"
  type        = string
  default     = "us-central1-a"
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
