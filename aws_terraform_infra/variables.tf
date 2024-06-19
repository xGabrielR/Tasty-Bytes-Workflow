variable "region" {
  type        = string
  default     = "us-east-1"
  description = "Default Aws Region for Infra"
}

variable "prefix" {
  type        = string
  default     = "grc"
  description = "Aws default prefix"
}

variable "bucket_names" {
  type        = list(string)
  description = "Aws default buckets"
  default = [
    "raw",     # parquet files
    "curated", # curated
    "scripts"  # scripts
  ]
}

locals {
  prefix = var.prefix
  common_tags = {
    Project     = "grc-tasty-bytes"
    Terraform   = true
    Environment = "dev"
  }
}
