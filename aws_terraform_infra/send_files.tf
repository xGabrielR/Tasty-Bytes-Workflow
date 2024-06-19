resource "aws_s3_bucket_object" "ingest_object" {
  bucket = "grc-scripts"
  key    = "job/tt_insert.py"
  source = "./glue_jobs/tt_insert.py"

  depends_on = [aws_s3_bucket.buckets]
}

resource "aws_s3_bucket_object" "view_object" {
  bucket = "grc-scripts"
  key    = "job/tt_view.py"
  source = "./glue_jobs/tt_view.py"

  depends_on = [aws_s3_bucket.buckets]
}
