variable "region" {
  description = "Project Region"
  default     = "us-central1"
}


variable "project" {
  description = "Project"
  default     = "terraform-415809"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "bigquery_dataset_name" {
  description = "BigQuery Dataset Name"
  default     = "demo_dataset"
}

variable "gsc_storage_bucket_class" {
  description = "Bucket Storge Class"
  default     = "STANDARD"
}

variable "gsc_bucket_name" {
  description = "MY Storge Bucket Name"
  default     = "terraform-415809-terra-bucket"
}

variable "credentials" {
  description = "Project credentials"
  default     = "/Users/mohmedshoab/test/terrademo/keys/my-creds.jason"
}

