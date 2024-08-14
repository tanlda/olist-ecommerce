# module "dynamodb" {
#   source = "terraform-aws-modules/dynamodb-table/aws"
#
#   name      = "olist-ecommerce-order-reviews"
#   hash_key  = "order_id"
#   range_key = "review_creation_date"
#
#   attributes = [
#     { name = "order_id", type = "S" },
#     { name = "review_creation_date", type = "S" },
#     # { name = "review_id", type = "S" },
#     # { name = "review_score", type = "N" },
#     # { name = "review_comment_title", type = "S" },
#     # { name = "review_comment_message", type = "S" },
#     # { name = "review_answer_timestamp", type = "S" },
#   ]
# }
