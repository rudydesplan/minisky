terraform {
  backend "local" {}

  required_version = ">= 1.7.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.41.0"
    }
  }
}

variable "endpoint" {
  description = "MiniSky public gateway origin."
  type        = string

  validation {
    condition     = can(regex("^http://(localhost|127\\.0\\.0\\.1):[0-9]+$", var.endpoint))
    error_message = "endpoint must be an HTTP localhost or IPv4 loopback origin with an explicit port."
  }
}

variable "project_id" {
  description = "Synthetic project used by the Redis compatibility fixture."
  type        = string
  default     = "local-dev-project"
}

variable "region" {
  description = "Synthetic region used by the Redis compatibility fixture."
  type        = string
  default     = "us-central1"
}

variable "instance_name" {
  description = "Exact Redis instance name for the durability gate."
  type        = string
  default     = "minisky-redis"
}

provider "google" {
  project      = var.project_id
  region       = var.region
  access_token = "minisky-local-only"

  redis_custom_endpoint = "${var.endpoint}/_minisky/redis/v1/"
}

resource "google_redis_instance" "durability_gate" {
  name                    = var.instance_name
  tier                    = "BASIC"
  memory_size_gb          = 1
  region                  = var.region
  redis_version           = "REDIS_7_2"
  connect_mode            = "DIRECT_PEERING"
  transit_encryption_mode = "DISABLED"
  auth_enabled            = false
  deletion_protection     = false

  timeouts {
    create = "3m"
    delete = "3m"
  }
}

output "instance_name" {
  value = google_redis_instance.durability_gate.name
}

output "host" {
  value = google_redis_instance.durability_gate.host
}

output "port" {
  value = google_redis_instance.durability_gate.port
}

output "redis_version" {
  value = google_redis_instance.durability_gate.redis_version
}
