locals {
  profile                = "default"
  region                 = "ap-southeast-1"
  backend_bucket         = "olist-ecommerce-state-bucket"
  backend_dynamodb_table = "olist-ecommerce-state-lock-table"
}

remote_state {
  backend  = "s3"
  generate = {
    path      = "backend.tf"
    if_exists = "overwrite_terragrunt"
  }
  config = {
    region         = "${local.region}"
    bucket         = "${local.backend_bucket}"
    dynamodb_table = "${local.backend_dynamodb_table}"
    key            = "terraform/${path_relative_to_include()}/terraform/terraform.tfstate"
    encrypt        = true
  }
}

generate "provider" {
  path      = "provider.tf"
  if_exists = "overwrite_terragrunt"
  contents  = <<EOF
provider "aws" {
    region  = "${local.region}"
    profile = "${local.profile}"
}
EOF
}
