variable "aws_region" {
  description = "AWS region to provision resources in."
  type        = string
  default     = "eu-central-1"
}

variable "github_repo" {
  description = "GitHub repo allowed to assume the OIDC role (owner/repo)."
  type        = string
  default     = "NikrrGit/Nexus"
}

variable "bucket_name" {
  description = "Name of the S3 bucket for dbt/Elementary artifacts."
  type        = string
  default     = "nexus-dbt-artifacts"
}
