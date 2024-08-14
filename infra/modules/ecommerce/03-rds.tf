module "rds" {
  source  = "terraform-aws-modules/rds/aws"
  version = "~> 6.3"

  identifier = "olist-ecommerce-rds"

  engine               = "postgres"
  family               = "postgres14"
  engine_version       = "14.12"
  major_engine_version = "14"
  instance_class       = "db.t3.micro"

  allocated_storage     = 5
  max_allocated_storage = 10

  username = "app"
  password = random_password.rds_password.result
  db_name  = "ecommerce"
  port     = 5432

  manage_master_user_password         = false
  allow_major_version_upgrade         = true
  iam_database_authentication_enabled = true

  multi_az             = false
  db_subnet_group_name = module.vpc.database_subnet_group_name
  vpc_security_group_ids = [module.rds_security_group.security_group_id]

  /*
    -- https://repost.aws/questions/QUpkrhcfkYQtS2adbjpQ7quQ/cannot-connect-from-glue-to-rds-postgres#ANzKC5VfBqSkqBR2dM-X0Stw
    SET password_encryption = 'md5';
    ALTER USER "app" WITH ENCRYPTED PASSWORD '${random_password.rds_password.result}';
   */

  skip_final_snapshot = true
  parameters = [
    {
      name  = "autovacuum"
      value = 1
    },
    {
      name  = "client_encoding"
      value = "utf8"
    }
  ]
}

resource "random_password" "rds_password" {
  length           = 16
  special          = true
  override_special = "_-=!"
}

module "rds_security_group" {
  source = "terraform-aws-modules/security-group/aws//modules/http-80"

  name   = "${module.vpc.name}-rds"
  vpc_id = module.vpc.vpc_id

  ingress_cidr_blocks = concat(
    [for subnet in aws_subnet.bastion_subnets : subnet.cidr_block],
    module.vpc.private_subnets_cidr_blocks,
    module.vpc.redshift_subnets_cidr_blocks,
  )
  ingress_rules = ["postgresql-tcp"]

  egress_cidr_blocks = ["0.0.0.0/0"]  # For glue
  egress_rules = ["all-all"]

  tags = {
    Env = var.env
  }
}
