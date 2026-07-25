terraform {
  required_version = ">= 1.9.0, < 2.0.0"

  backend "local" {}

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.41.0"
    }
  }
}
