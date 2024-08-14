module "bucket" {
  source = "terraform-aws-modules/s3-bucket/aws"

  bucket = "olist-ecommerce-bucket"

  block_public_acls       = false
  block_public_policy     = false
  ignore_public_acls      = false
  restrict_public_buckets = false

  cors_rule = jsonencode([
    {
      allowed_origins = ["*"]
      allowed_headers = ["*"]
      allowed_methods = ["GET", "HEAD", "PUT"]
      expose_headers = ["ETag"]
      max_age_seconds = 3000
    }
  ])

  control_object_ownership = true
  object_ownership         = "ObjectWriter"
}
