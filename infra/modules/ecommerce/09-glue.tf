data "aws_region" "current" {}

# IAM
resource "aws_iam_role" "glue_etl_role" {
  name = "aws-glue-etl-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["sts:AssumeRole"]
        Principal = {
          Service = "glue.amazonaws.com"
        }
      },
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_etl_glue_rpa" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
  role       = aws_iam_role.glue_etl_role.name
}

resource "aws_iam_role_policy_attachment" "glue_etl_rds_rpa" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonRDSReadOnlyAccess"
  role       = aws_iam_role.glue_etl_role.name
}

resource "aws_iam_role_policy_attachment" "glue_etl_s3_rpa" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"  # TODO: bucket
  role = aws_iam_role.glue_etl_role.name
}

resource "aws_iam_role_policy_attachment" "glue_etl_dynamodb_rpa" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess"  # TODO: table
  role = aws_iam_role.glue_etl_role.name
}

resource "aws_iam_policy" "glue_etl_pass_role_policy" {
  name = "aws-glue-pass-role-policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["iam:PassRole"]
        Resource = [aws_iam_role.glue_etl_role.arn]
        Condition = {
          StringLike = {
            "iam:PassedToService" = ["glue.amazonaws.com"]
          }
        }
      },
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_etl_pass_role_rpa" {
  policy_arn = aws_iam_policy.glue_etl_pass_role_policy.arn
  role       = aws_iam_role.glue_etl_role.name
}

resource "aws_iam_role_policy_attachment" "glue_etl_nb_rpa" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceNotebookRole"
  role       = aws_iam_role.glue_etl_role.name
}

resource "aws_iam_role_policy_attachment" "glue_etl_redshift_rpa" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonRedshiftFullAccess"  # TODO: cluster
  role = aws_iam_role.glue_etl_role.name
}

# CATALOG
resource "aws_glue_catalog_database" "ecommerce" {
  name = "ecommerce"
}

# CONNECTIONS
resource "aws_security_group_rule" "rds_sg_self_referencing_rule" {
  security_group_id = module.rds_security_group.security_group_id
  description       = "Self-Ingress Rule"
  type              = "ingress"
  protocol          = "tcp"
  from_port         = 0
  to_port           = 65535
  self              = true
}

resource "aws_glue_connection" "glue_rds_connection" {
  name            = "ecommerce-rds-connection"
  connection_type = "JDBC"

  connection_properties = {
    USERNAME = module.rds.db_instance_username
    PASSWORD = random_password.rds_password.result

    CONNECTOR_TYPE      = "JDBC"
    JDBC_ENFORCE_SSL    = "false"
    JDBC_CONNECTION_URL = "jdbc:postgresql://${module.rds.db_instance_address}:${module.rds.db_instance_port}/${module.rds.db_instance_name}"
  }

  physical_connection_requirements {
    availability_zone = module.vpc.azs[0]
    security_group_id_list = [module.rds_security_group.security_group_id]
    subnet_id         = module.vpc.database_subnets[0]
  }

  depends_on = [
    aws_security_group_rule.rds_sg_self_referencing_rule,
  ]
}

resource "aws_glue_connection" "glue_redshift_connection" {
  name            = "ecommerce-redshift-connection"
  connection_type = "JDBC"

  connection_properties = {
    USERNAME = "app"
    PASSWORD = random_password.redshift_password.result

    CONNECTOR_TYPE      = "JDBC"
    JDBC_ENFORCE_SSL    = "true"
    JDBC_CONNECTION_URL = "jdbc:redshift://${module.redshift.cluster_hostname}:${module.redshift.cluster_port}/${module.redshift.cluster_database_name}"
  }

  physical_connection_requirements {
    availability_zone = module.vpc.azs[0]
    security_group_id_list = [module.redshift_security_group.security_group_id]
    subnet_id         = module.vpc.redshift_subnets[0]  # TODO
  }

  depends_on = [
    module.redshift,
  ]
}

# CRAWLERS
resource "aws_glue_crawler" "glue_crawler" {
  database_name = aws_glue_catalog_database.ecommerce.name
  role          = aws_iam_role.glue_etl_role.name
  name          = "ecommerce-crawler"

  jdbc_target {
    connection_name = aws_glue_connection.glue_rds_connection.name
    path            = "${module.rds.db_instance_name}/%"
  }

  s3_target {
    path        = "s3://${module.bucket.s3_bucket_id}/dms/ecommerce/order_reviews"
    sample_size = 10
  }

  depends_on = [
    aws_glue_connection.glue_rds_connection
  ]
}

locals {
  tables = [
    "customers",
    "orders",
  ]

  dirs = [
    "logs/",
    "staging/",
    "bookmarks/",
  ]
}

# ETL Scripts
resource "aws_s3_object" "glue_rds_redshift_scripts" {
  for_each = toset(local.tables)

  bucket = module.bucket.s3_bucket_id
  key    = "glue/scripts/rds_redshift_${each.value}_etl.py"
  source = "glue/scripts/rds_redshift_${each.value}_etl.py"
  etag = filemd5("glue/scripts/rds_redshift_${each.value}_etl.py")
}

# ETL Folders
resource "aws_s3_object" "glue_s3_path" {
  for_each = toset(local.dirs)
  bucket = module.bucket.s3_bucket_id
  key    = "glue/${each.value}"
}

# ETL Jobs
resource "aws_glue_job" "glue_rds_redshift_jobs" {
  for_each = toset(local.tables)

  name     = "rds_redshift_${each.value}_etl"
  role_arn = aws_iam_role.glue_etl_role.arn

  worker_type       = "G.1X"
  glue_version      = "4.0"
  execution_class   = "FLEX"
  max_retries       = 0
  number_of_workers = 2
  timeout           = 30


  command {
    name            = "glueetl"
    script_location = "s3://${module.bucket.s3_bucket_id}/glue/scripts/rds_redshift_${each.value}_etl.py"
    python_version  = "3"
  }

  connections = [
    aws_glue_connection.glue_rds_connection.name,
    aws_glue_connection.glue_redshift_connection.name,
  ]

  default_arguments = {
    "--job-language" = "python"
  }

  depends_on = [
    aws_s3_object.glue_rds_redshift_scripts
  ]
}
