# AWS Serverless Event-Driven ETL Pipeline

![Architecture Diagram](ETL_Architecture_SS.png)

## Project Overview

This project implements a scalable, fully automated ETL pipeline designed to transform raw, nested JSON data into an analytics-ready Data Lake on AWS.

## Architecture

Key Services: S3, Lambda, Glue, Athena, Python (Pandas), IAM, Cloudwatch

1.  Ingest:Raw JSON transaction data is uploaded to an Amazon S3 bucket.
2.  Trigger: An S3 Event Notification immediately triggers an AWS Lambda function.
3.  Process:The Lambda function (using the Pandas library via AWS Layers) creates a DataFrame, flattens the nested JSON structure, and converts the data into Parquet format.
4.  Store:The optimized Parquet files are stored in a target S3 bucket (Data Lake).
5.  Catalog: AWS Glue Crawlers are programmatically triggered to update the Data Catalog with new schemas and partitions.
6.  Analyze: Data is immediately available for querying via Amazon Athena.
