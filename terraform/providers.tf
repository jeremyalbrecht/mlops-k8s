terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}
provider "aws" {
  access_key = var.rustfs_access_key
  secret_key = var.rustfs_secret_key
  region     = "us-east-1"
  # Point to the RustFS S3-compatible endpoint
  endpoints {
    s3 = var.rustfs_endpoint
  }
  # Required for non-AWS S3-compatible stores
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
  s3_use_path_style = true
}
