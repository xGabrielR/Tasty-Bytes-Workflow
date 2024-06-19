resource "aws_glue_job" "ingest_glue_job" {
  name              = "tt_csv_to_parquet"
  role_arn          = aws_iam_role.glue_job.arn
  glue_version      = "4.0"
  worker_type       = "Standard"
  number_of_workers = 4
  timeout           = 50

  command {
    script_location = "s3://grc-scripts/job/tt_insert.py"
  }

  tags = local.common_tags
}

resource "aws_glue_job" "view_glue_job" {
  name              = "tt_view"
  role_arn          = aws_iam_role.glue_job.arn
  glue_version      = "4.0"
  worker_type       = "Standard"
  number_of_workers = 2
  timeout           = 50

  command {
    script_location = "s3://grc-scripts/job/tt_view.py"
  }

  tags = local.common_tags
}
