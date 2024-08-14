module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.8"

  name = "olist-ecommerce-vpc"
  cidr = "10.0.0.0/16"
  azs = ["ap-southeast-1a", "ap-southeast-1b"]
  public_subnets = ["10.0.1.0/24", "10.0.2.0/24"]
  private_subnets = ["10.0.11.0/24", "10.0.12.0/24"]
  database_subnets = ["10.0.21.0/24", "10.0.22.0/24"]
  redshift_subnets = ["10.0.41.0/24", "10.0.42.0/24"]
  intra_subnets = ["10.0.51.0/24", "10.0.52.0/24"]

  enable_nat_gateway     = false
  single_nat_gateway     = false
  one_nat_gateway_per_az = false
  enable_public_redshift = false

  tags = {
    Env = var.env
  }
}

module "vpc_endpoints" {
  source = "terraform-aws-modules/vpc/aws//modules/vpc-endpoints"
  vpc_id = module.vpc.vpc_id

  endpoints = {
    s3 = {
      service      = "s3"
      service_type = "Gateway"
      tags = { Name = "olist-ecommerce-s3" }

      route_table_ids = concat(
        module.vpc.database_route_table_ids,
        module.vpc.private_route_table_ids,
        module.vpc.intra_route_table_ids,
      )

      policy = jsonencode({
        Version = "2012-10-17",
        Statement = [
          {
            Effect    = "Allow",
            Principal = "*",
            Action = [
              "s3:*",
            ],
            Resource = [
              module.bucket.s3_bucket_arn,
              "${module.bucket.s3_bucket_arn}/*"
            ]
          }
        ]
      })
    }

    dynamodb = {
      service      = "dynamodb"
      service_type = "Gateway"
      tags = { Name = "olist-ecommerce-dynamodb" }

      route_table_ids = concat(
        module.vpc.database_route_table_ids,
        module.vpc.private_route_table_ids,
        module.vpc.intra_route_table_ids,
      )

      policy = jsonencode({
        Version = "2012-10-17",
        Statement = [
          {
            Effect    = "Allow",
            Principal = "*",
            Action = [
              "dynamodb:*",
            ],
            Resource = [
              "*"
            ]
          }
        ]
      })
    },
  }
}
