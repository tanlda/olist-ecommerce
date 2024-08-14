module "redshift" {
  source = "terraform-aws-modules/redshift/aws"

  cluster_identifier    = "olist-ecommerce-redshift"
  allow_version_upgrade = true
  node_type             = "dc2.large"
  number_of_nodes       = 1

  database_name          = "ecommerce"
  master_username        = "app"
  master_password        = random_password.redshift_password.result
  create_random_password = false

  enhanced_vpc_routing = true
  vpc_security_group_ids = [module.redshift_security_group.security_group_id]
  subnet_ids = [module.vpc.redshift_subnets[0]]

  iam_role_arns = [
    aws_iam_role.redshift_role.arn,
  ]

  tags = {
    Env = var.env
  }
}

resource "aws_iam_role" "redshift_role" {
  name = "aws-redshift-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "sts:AssumeRole"
        ],
        Principal = {
          Service = "redshift.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_policy" "redshift_s3_policy" {
  name = "aws-redshift-policy"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "s3:*",
        ],
        Resource = [
          "*", # TODO:
        ],
      },
    ],
  })
}

resource "aws_iam_role_policy_attachment" "redshift_s3_rpa" {
  policy_arn = aws_iam_policy.redshift_s3_policy.arn
  role       = aws_iam_role.redshift_role.name
}

resource "aws_iam_role_policy_attachment" "redshift_glue_rpa" {
  policy_arn = "arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess"
  role       = aws_iam_role.redshift_role.name
}

resource "aws_iam_role_policy_attachment" "redshift_athena_rpa" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonAthenaFullAccess"
  role       = aws_iam_role.redshift_role.name
}

resource "random_password" "redshift_password" {
  length           = 16
  special          = true
  override_special = "!#$%&*()-_=+[]{}<>:?"
}

module "redshift_security_group" {
  source = "terraform-aws-modules/security-group/aws//modules/http-80"

  vpc_id = module.vpc.vpc_id
  name   = "${module.vpc.name}-redshift"

  ingress_cidr_blocks = concat(
    module.vpc.intra_subnets_cidr_blocks,
    module.vpc.private_subnets_cidr_blocks,
    module.vpc.database_subnets_cidr_blocks,
  )
  ingress_rules = ["redshift-tcp"]

  egress_cidr_blocks = ["0.0.0.0/0"]
  egress_rules = ["all-all"]

  depends_on = [
    module.vpc
  ]
}
