output "bastion" {
  value = {
    host = module.bastion.public_ip
  }
}

output "rds" {
  value = {
    host     = module.rds.db_instance_address
    username = module.rds.db_instance_username
    password = random_password.rds_password.result
  }

  sensitive = true
}

output "redshift" {
  value = {
    host     = module.redshift.cluster_hostname
    password = random_password.redshift_password.result
  }

  sensitive = true
}
