resource "aws_glue_workflow" "tt_workflow" {
  name                = "tt_workflow"
  description         = "Tasty Bytes (CSV TO PARQUET) and (PARQUET TO Views)."
  max_concurrent_runs = 1

  tags = local.common_tags
}

resource "aws_glue_trigger" "glue_first_trigger" {
  name          = "daily_csv_to_parquet"
  type          = "ON_DEMAND"
  workflow_name = aws_glue_workflow.tt_workflow.name

  actions {
    job_name = aws_glue_job.ingest_glue_job.name
  }

  tags = local.common_tags

}

resource "aws_glue_trigger" "after_trigger" {
  name          = "conditional_daily_csv_to_parquet_view"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.tt_workflow.name

  predicate {
    conditions {
      job_name = aws_glue_job.ingest_glue_job.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = aws_glue_job.view_glue_job.name
  }

  tags = local.common_tags
}
