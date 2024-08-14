module "dms" {
  source  = "terraform-aws-modules/dms/aws"
  version = "~> 2.0"
  create = false

  # Subnet group
  repl_subnet_group_name        = "dms"
  repl_subnet_group_description = "dms subnet group description"
  repl_subnet_group_subnet_ids = [for subnet in aws_subnet.bastion_subnets : subnet.id]

  # Instance
  repl_instance_allocated_storage           = 6
  repl_instance_auto_minor_version_upgrade  = true
  repl_instance_allow_major_version_upgrade = true
  repl_instance_apply_immediately           = true
  repl_instance_engine_version              = "3.5.3"
  repl_instance_multi_az                    = false
  repl_instance_publicly_accessible         = true
  repl_instance_class                       = "dms.t2.micro"
  repl_instance_id                          = "dms"
  repl_instance_vpc_security_group_ids = [module.bastion_security_group.security_group_id]

  # Permissions
  create_access_policy   = true
  create_access_iam_role = true
  access_iam_role_name   = "dms-role"
  access_iam_role_policies = {
    s3 = aws_iam_policy.dms_s3_policy.arn
  }

  endpoints = {
    database = {
      database_name = "ecommerce"
      endpoint_id   = "database"
      endpoint_type = "source"
      engine_name   = "postgres"
      username      = "admin"
      password      = "password"
      server_name   = "database.olist-ecommerce.co"
      port          = 5432
      ssl_mode      = "none"
      tags = { EndpointType = "source", Engine = "postgres" }
    }

    document = {
      database_name = "ecommerce"
      endpoint_type = "source"
      endpoint_id   = "document"
      engine_name   = "mongodb"
      username      = "admin"
      password      = "password"
      server_name   = "document.olist-ecommerce.co"
      port          = 27017
      ssl_mode      = "none"
      mongodb_settings = {
        auth_type      = "password"
        auth_source    = "admin"
        auth_mechanism = "default"
        # table mode
        # https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Source.MongoDB.html
        # https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/dms_endpoint#nesting_level
        nesting_level  = "one"
      }
      tags = { EndpointType = "source", Engine = "mongodb" }
    }

    rds = {
      database_name = "ecommerce"
      endpoint_id   = "rds"
      endpoint_type = "target"
      engine_name   = "postgres"
      username      = module.rds.db_instance_username
      password      = random_password.rds_password.result
      port          = module.rds.db_instance_port
      server_name   = module.rds.db_instance_address
      ssl_mode      = "none"
      tags = { EndpointType = "target", Engine = "postgres" }
    }

    # sss = {}
  }

  replication_tasks = {
    database_rds_full_load = {
      replication_task_id = "database-rds-full-load"
      migration_type      = "full-load"
      replication_task_settings = file("dms/database_rds/full_load_task_settings.json")
      table_mappings = file("dms/database_rds/full_load_table_mappings.json")
      source_endpoint_key = "database"
      target_endpoint_key = "rds"
    }

    # database_rds_cdc = {
    #   replication_task_id = "database-rds-cdc"
    #   migration_type      = "cdc"
    #   replication_task_settings = file("dms/database_rds/cdc_task_settings.json")
    #   table_mappings = file("dms/database_rds/cdc_table_mappings.json")
    #   source_endpoint_key = "database"
    #   target_endpoint_key = "rds"
    # }

    # document_sss_full_load = {
    #   replication_task_id = "document-sss-full-load"
    #   migration_type      = "full-load"
    #   replication_task_settings = file("dms/document_sss/full_load_task_settings.json")
    #   table_mappings = file("dms/document_sss/full_load_table_mappings.json")
    #   source_endpoint_key = "document"
    #   target_endpoint_key = "sss"
    # }

    # document_sss_cdc = {
    #   replication_task_id = "postgres-sss-cdc"
    #   migration_type      = "cdc"
    #   replication_task_settings = file("dms/document_sss/cdc_task_settings.json")
    #   table_mappings = file("dms/document_sss/cdc_table_mappings.json")
    #   source_endpoint_key = "document"
    #   target_endpoint_key = "sss"
    # }
  }

  event_subscriptions = {
    # sns
  }

  tags = {
    Env = var.env
  }

  depends_on = [
    module.vpc,
    module.rds,
  ]
}

resource "aws_dms_replication_task" "document_sss_full_load" {
  count = 0

  replication_task_id      = "document-sss-full-load"
  migration_type           = "full-load"
  replication_instance_arn = module.dms.replication_instance_arn
  source_endpoint_arn      = module.dms.endpoints["document"].endpoint_arn
  target_endpoint_arn      = aws_dms_s3_endpoint.sss[0].endpoint_arn

  replication_task_settings = file("dms/document_sss/full_load_task_settings.json")
  table_mappings = file("dms/document_sss/full_load_table_mappings.json")
}

resource "aws_dms_s3_endpoint" "sss" {
  count = 0

  endpoint_id             = "sss"
  bucket_folder           = "dms"
  endpoint_type           = "target"
  bucket_name             = module.bucket.s3_bucket_id
  service_access_role_arn = module.dms.access_iam_role_arn

  add_column_name = true
  data_format     = "csv"
}

# https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.AssessmentReport.Prerequisites.html
resource "aws_iam_policy" "dms_s3_policy" {
  name = "dms-s3-policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:PutObjectTagging",
        ]
        Resource = [
          "${module.bucket.s3_bucket_arn}/dms/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          module.bucket.s3_bucket_arn
        ]
      }
    ]
  })
}
