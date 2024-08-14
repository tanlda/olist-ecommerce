variable "profile" {
  type    = string
  default = "default"
}

variable "region" {
  type    = string
  default = "ap-southeast-1"
}

variable "state_bucket" {
  type    = string
  default = "olist-ecommerce-state-bucket"
}

variable "state_lock_table" {
  type    = string
  default = "olist-ecommerce-state-lock-table"
}

variable "sse_algorithm" {
  type    = string
  default = "aws:kms"
}
