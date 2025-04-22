# Data Lake with Scalable Architectures

## Overview
Repository for designing and implementing a scalable Data Lake for semi-structured and unstructured data, using Azure Data Lake and Delta Lake.

## Objectives
- Design ingestion and query layers.
- Implement best practices for partitioning and scalability.
- Provide read/write examples with Spark and Python.
- Include monitoring, alerting, and data quality validation.

## Tech stack
- **Storage**: Azure Data Lake Storage Gen2
- **Processing**: Apache Spark (pyspark)
- **Format**: Delta Lake / Parquet
- **Language**: Python 3.x
- **Validation**: Great Expectations, AWS Deequ
- **Monitoring**: Azure Monitor with Kusto

## Repository structure
\`\`\`
.
├── README_en.md
├── src/
│   ├── ingestion/
│   │   ├── batch_ingestion.py
│   │   └── streaming_ingestion.py
│   ├── transformations/
│   │   └── transform.py
│   ├── validation/
│   │   ├── great_expectations/
│   │   └── deequ/
│   └── monitoring/
│       └── alerts.kql
├── docs/
│   └── architecture.md
└── config/
    ├── spark_conf.yaml
    └── azure_storage.json
\`\`\`

## Datasets
- **Rental contracts**: \`dld_rent_contracts-open\` from Dubai Land Department (Ejari).
- **Transactions**: \`dld_transactions-open\` from Dubai Land Department.

## Partitioning scheme
- Partitioned by: date (\`year\`, \`month\`, \`day\`), \`emirate\`, \`property_type\`
- Format: Delta Lake / Parquet

## Ingestion pipelines
### Batch
- Frequency: daily at 00:30 AM UTC+2.
- Script: \`src/ingestion/batch_ingestion.py\`
- Description: downloads datasets, applies schemas, writes partitions.

### Streaming (micro-batches)
- Trigger: every 24 hours.
- Script: \`src/ingestion/streaming_ingestion.py\`
- Description: captures new records and writes at end of day.

## Monitoring and alerts
- Metrics: number of ingested records, latency, job errors.
- Alerts: Azure Monitor with KQL.
- Example alert (duration > 1h, error count > 0) in \`config/alerts.kql\`.

## Data validation
- **Great Expectations**: schema and null checks in \`src/validation/great_expectations/\`.
- **Deequ**: quality checks in \`src/validation/deequ/\`.

## Code examples
### Spark read (Python)
\`\`\`python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("DataLakeIngestion")
         .getOrCreate())

df = (spark.read.format("delta")
      .load("abfss://datalake@youraccount.dfs.core.windows.net/path/to/delta/dld_rent_contracts-open")
      .filter("year = 2025 and month = 4 and emirate = 'Dubai'"))
df.show(5)
\`\`\`

### Spark write
\`\`\`python
(df.write.format("delta")
    .mode("append")
    .partitionBy("year", "month", "day", "emirate", "property_type")
    .save("abfss://datalake@youraccount.dfs.core.windows.net/path/to/delta/dld_transactions-open"))
\`\`\`

## Deployment
- Configure credentials in \`config/azure_storage.json\`.
- Adjust Spark settings in \`config/spark_conf.yaml\`.
- Run \`src/ingestion/batch_ingestion.py\` on Azure Databricks or Synapse.
