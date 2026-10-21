# aws-health-encounters-etl
End-to-end AWS ETL pipeline for healthcare encounter data. Glue + S3 + Athena, with year/month partitioning (96% less data scanned), 8 data quality rules with a quarantine path, safe reruns with no duplicate rows, and failure alerts.
