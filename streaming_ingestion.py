from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth
import json

# Load configuration
with open('config/azure_storage.json') as f:
    storage_conf = json.load(f)

storage_account = storage_conf['account_name']
storage_key = storage_conf['account_key']
container = storage_conf['container_name']

# Streaming source URLs (same as batch)
DLD_RENT_URL = 'https://www.dubaipulse.gov.ae/api/v1/datasets/dld_rent_contracts-open/data?format=csv'
DLD_TRANS_URL = 'https://www.dubaipulse.gov.ae/api/v1/datasets/dld_transactions-open/data?format=csv'

# Spark session initialization
spark = (SparkSession.builder
         .appName("DataLakeStreamingIngestion")
         .config(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", storage_key)
         .getOrCreate())

# Define streaming ingestion function
def ingest_streaming(url, delta_path):
    stream_df = (spark.readStream
                 .option("header", "true")
                 .option("inferSchema", "true")
                 .csv(url))
    
    date_col = 'contract_date' if 'rent' in delta_path else 'transaction_date'
    stream_df = (stream_df.withColumn("year", year(stream_df[date_col]))
                            .withColumn("month", month(stream_df[date_col]))
                            .withColumn("day", dayofmonth(stream_df[date_col])))
    
    (stream_df.writeStream
              .format("delta")
              .outputMode("append")
              .option("checkpointLocation", f"{delta_path}/_checkpoints")
              .partitionBy("year", "month", "day", "emirate", "property_type")
              .trigger(processingTime="24 hours")
              .start(delta_path))

# Paths in ADLS Gen2 (Delta Lake)
base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/delta"
rent_delta_path = f"{base_path}/dld_rent_contracts-open"
trans_delta_path = f"{base_path}/dld_transactions-open"

# Start streaming ingestion
ingest_streaming(DLD_RENT_URL, rent_delta_path)
ingest_streaming(DLD_TRANS_URL, trans_delta_path)

spark.streams.awaitAnyTermination()
