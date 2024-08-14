include "root" {
  path = find_in_parent_folders()
}

terraform {
  source = "../../..//modules/ecommerce"
}

locals {
  env    = read_terragrunt_config(find_in_parent_folders("env.hcl"))
  common = read_terragrunt_config(find_in_parent_folders("common.hcl"))
}

inputs = merge(
  local.env.inputs,
  local.common.inputs,
  {
  }
)
