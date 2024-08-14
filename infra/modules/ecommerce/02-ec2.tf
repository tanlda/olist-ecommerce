data "aws_ami" "ubuntu" {
  most_recent = true

  filter {
    name = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }

  filter {
    name = "virtualization-type"
    values = ["hvm"]
  }

  owners = ["099720109477"] # Canonical
}

resource "aws_key_pair" "bastion" {
  key_name = "olist-ecommerce-bastion"
  public_key = file("~/.ssh/id_ed25519.pub")
}

module "bastion" {
  source = "terraform-aws-modules/ec2-instance/aws"

  ami  = data.aws_ami.ubuntu.id
  name = "olist-ecommerce-bastion"

  instance_type = "t2.micro"
  key_name      = aws_key_pair.bastion.key_name
  subnet_id     = aws_subnet.bastion_subnets["a"].id
  vpc_security_group_ids = [module.bastion_security_group.security_group_id]

  associate_public_ip_address = true

  user_data = <<EOF
    #!/bin/bash
    sudo apt-get update -y
    sudo apt-get install -y postgresql-client-common postgresql-client-14
  EOF

  tags = {
    Env = var.env
  }
}

resource "aws_subnet" "bastion_subnets" {
  for_each = { a = 91, b = 92 }

  vpc_id            = module.vpc.vpc_id
  cidr_block        = "10.0.${each.value}.0/24"
  availability_zone = "ap-southeast-1${each.key}"

  tags = {
    Name = "${module.vpc.name}-bastion-ap-southeast-1${each.key}"
  }
}

resource "awscc_ec2_subnet_route_table_association" "bastion_subnet_associations" {
  for_each       = {for idx, subnet in aws_subnet.bastion_subnets : idx => subnet}
  route_table_id = module.vpc.public_route_table_ids[0]
  subnet_id      = each.value.id
}

module "bastion_security_group" {
  source = "terraform-aws-modules/security-group/aws//modules/http-80"

  name   = "${module.vpc.name}-bastion"
  vpc_id = module.vpc.vpc_id

  ingress_cidr_blocks = ["0.0.0.0/0"]
  ingress_rules = ["ssh-tcp"]

  egress_cidr_blocks = ["0.0.0.0/0"]
  egress_rules = ["all-all"]

  tags = {
    Env = var.env
  }
}
