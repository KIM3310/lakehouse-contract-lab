# =============================================================================
# Lakehouse Contract Lab - Terraform Outputs
# =============================================================================

# --- AWS ---

output "s3_bucket_name" {
  description = "Name of the S3 artifact bucket"
  value       = aws_s3_bucket.artifacts.id
}

output "s3_bucket_arn" {
  description = "ARN of the S3 artifact bucket"
  value       = aws_s3_bucket.artifacts.arn
}

output "pipeline_role_arn" {
  description = "ARN of the IAM role for pipeline execution"
  value       = aws_iam_role.pipeline.arn
}

# --- GCP ---

output "cloud_run_url" {
  description = "Public URL of the Cloud Run service"
  value       = google_cloud_run_v2_service.app.uri
}

output "cloud_run_service_name" {
  description = "Name of the Cloud Run service"
  value       = google_cloud_run_v2_service.app.name
}
