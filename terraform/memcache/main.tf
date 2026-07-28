variable "minisky_endpoint" {
  type = string
}

variable "project_id" {
  type = string
}

variable "region" {
  type = string
}

variable "instance_name" {
  type = string
}

locals {
  minisky_base_url = trimsuffix(var.minisky_endpoint, "/")
}

provider "google" {
  project      = var.project_id
  region       = var.region
  access_token = "minisky-local-token"

  memcache_custom_endpoint = "${local.minisky_base_url}/_minisky/memcache/v1/"
}

resource "google_memcache_instance" "compatibility" {
  name         = var.instance_name
  region       = var.region
  display_name = "MiniSky Memcached updated"
  node_count   = 1

  node_config {
    cpu_count      = 1
    memory_size_mb = 1024
  }

  memcache_version    = "MEMCACHE_1_6_15"
  deletion_protection = false

  timeouts {
    create = "3m"
    update = "3m"
    delete = "3m"
  }
}

output "instance_name" {
  value = google_memcache_instance.compatibility.id
}
