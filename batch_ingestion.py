from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth
import json

# Load configuration
with open('config/azure_storage.json') as f:
    storage_conf = json.load(f)

storage_account = storage_conf['account_name']
storage_key = storage_conf['account_key']
container = storage_conf['container_name']

# URLs for DLD datasets
DLD_RENT_URL = 'https://www.dubaipulse.gov.ae/api/v1/datasets/dld_rent_contracts-open/data?format=csv'
DLD_TRANS_URL = 'https://www.dubaipulse.gov.ae/api/v1/datasets/dld_transactions-open/data?format=csv'

# Spark session initialization
spark = (SparkSession.builder
         .appName("DataLakeBatchIngestion")
         .config(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", storage_key)
         .getOrCreate())

# Function to ingest and write a dataset to Delta Lake
def ingest_to_delta(url, delta_path):
    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(url))
    
    # Add partition columns based on contract_date or transaction_date
    date_col = 'contract_date' if 'rent' in delta_path else 'transaction_date'
    df = (df.withColumn("year", year(df[date_col]))
            .withColumn("month", month(df[date_col]))
            .withColumn("day", dayofmonth(df[date_col])))
    
    # Write to Delta with partitioning
    (df.write
       .format("delta")
       .mode("overwrite")
       .partitionBy("year", "month", "day", "emirate", "property_type")
       .save(delta_path))

# Paths in ADLS Gen2 (Delta Lake)
base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/delta"
rent_delta_path = f"{base_path}/dld_rent_contracts-open"
trans_delta_path = f"{base_path}/dld_transactions-open"

# Ingest datasets
ingest_to_delta(DLD_RENT_URL, rent_delta_path)
ingest_to_delta(DLD_TRANS_URL, trans_delta_path)

spark.stop()
