# =============================================================================
# Lakehouse Contract Lab - Terraform Variables
# =============================================================================

variable "project_name" {
  description = "Project identifier used for resource naming"
  type        = string
  default     = "lakehouse-contract-lab"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod."
  }
}

# --- AWS ---

variable "aws_region" {
  description = "AWS region for S3 bucket and IAM resources"
  type        = string
  default     = "us-east-1"
}

variable "s3_force_destroy" {
  description = "Allow Terraform to destroy the S3 bucket even if it contains objects"
  type        = bool
  default     = false
}

# --- GCP ---

variable "gcp_project_id" {
  description = "GCP project ID for Cloud Run deployment"
  type        = string
}

variable "gcp_region" {
  description = "GCP region for Cloud Run service"
  type        = string
  default     = "us-central1"
}

variable "cloud_run_image" {
  description = "Container image URI for the Cloud Run service"
  type        = string
  default     = "gcr.io/lakehouse-contract-lab/app:latest"
}

variable "cloud_run_cpu" {
  description = "CPU allocation for Cloud Run container"
  type        = string
  default     = "1000m"
}

variable "cloud_run_memory" {
  description = "Memory allocation for Cloud Run container"
  type        = string
  default     = "2Gi"
}

variable "cloud_run_max_instances" {
  description = "Maximum number of Cloud Run instances"
  type        = number
  default     = 3
}

variable "cloud_run_min_instances" {
  description = "Minimum number of Cloud Run instances (0 allows scale to zero)"
  type        = number
  default     = 0
}

# --- Tags ---

variable "tags" {
  description = "Common tags applied to all AWS resources"
  type        = map(string)
  default     = {}
}
