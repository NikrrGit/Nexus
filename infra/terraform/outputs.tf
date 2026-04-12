output "artifact_bucket_name" {
  description = "S3 bucket where CI uploads Elementary reports."
  value       = aws_s3_bucket.artifacts.bucket
}

output "github_actions_role_arn" {
  description = "IAM role ARN — add this to GitHub Secrets as AWS_ROLE_TO_ASSUME."
  value       = aws_iam_role.github_actions.arn
}
