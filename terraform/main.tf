provider "aws" {
  region = var.aws_region
}

# AWS Managed EKS Cluster for Flink Workers
module "eks" {
  source          = "terraform-aws-modules/eks/aws"
  version         = "19.15.0"
  cluster_name    = "factory-iot-streaming"
  cluster_version = "1.27"
  
  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets
  
  eks_managed_node_groups = {
    flink_workers = {
      min_size     = 4
      max_size     = 20
      desired_size = 8
      instance_types = ["m5.2xlarge"] # Memory-optimized instance shapes for RocksDB State Backend
    }
  }
}

# AWS MSK (Managed Streaming Kafka)
resource "aws_msk_cluster" "kafka" {
  cluster_name           = "telemetry-event-backbone"
  kafka_version          = "3.6.0"
  number_of_broker_nodes = 3
  
  broker_node_group_info {
    instance_type = "kafka.m5.xlarge"
    client_subnets = module.vpc.private_subnets
    security_groups = [aws_security_group.kafka.id]
  }
}
