# module "pipeline" {
#   source = "terraform-aws-modules/step-functions/aws"
#
#   name       = "pipeline"
#   definition = <<EOF
# {
#   "
# }
#   EOF
#
#   service_integrations = {
#     s3 = { s3 = true }
#     glue = { glue = true }
#   }
# }
