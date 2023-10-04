terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "4.0.0"
    }
  }
}

provider "aws" {
  region = "eu-west-1"
}

# Create an infrastructure with System Under Test (SUT).
module "setup_infra" {
  source = "./modules/setup"

  # specify variables required to provide SUT
  some_sut_related_var = var.some_sut_related_var
  provider_access_token = var.provider_access_token
}

# Run the ec2 instance to create load. And run load scripts.
module "script" {
  source = "./modules/script"

  # variables to create ec2 instance to create load
  ami_id               = var.ami_id
  instance_type        = var.instance_type
  instance_user        = var.instance_user
  security_group_id    = var.security_group_id
  subnet_id            = var.subnet_id
  sut_name             = var.sut_name
  key_name             = var.key_name
  private_key_location = var.private_key_location
  num_instances        = var.num_instances

  # these will be passed to the script by supabench
  testrun_name    = var.testrun_name
  testrun_id      = var.testrun_id
  test_origin     = var.test_origin
  benchmark_id    = var.benchmark_id
  supabench_token = var.supabench_token
  supabench_uri   = var.supabench_uri

  # variables to pass to load script
  duration  = var.duration
  vus       = var.vus
  sut_url   = var.sut_url
  sut_username = var.sut_username
  sut_password = var.sut_password
  sut_query = var.sut_query
  sut_ns = var.sut_ns
  sut_db = var.sut_db

  depends_on = [
    module.setup_infra.ready,
  ]
}
