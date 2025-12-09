import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
AZURITE_ACCOUNT = "devstoreaccount1"
AZURITE_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
BLOB_CONTAINER = "datalake"

# Note: "azurite" must be the service name in your docker-compose file
AZURITE_BLOB_ENDPOINT = f"http://azurite:10000/{AZURITE_ACCOUNT}"
WASBS_PATH = f"wasb://{BLOB_CONTAINER}@{AZURITE_ACCOUNT}.blob.core.windows.net"

DB_URL = "jdbc:postgresql://timescaledb:5432/iiot" 
DB_USER = "admin"
DB_PASSWORD = "admin"
DB_DRIVER = "org.postgresql.Driver"

TABLES_TO_EXTRACT = ["machine_sensors", "sensor_aggregates"]

def create_spark_session():
    try:
        # Configuration keys for Azure Blob Storage (WASB)
        # Note: spark.hadoop. prefix is REQUIRED for these to be passed to the Hadoop FileSystem
        account_key_conf = f"spark.hadoop.fs.azure.account.key.{AZURITE_ACCOUNT}.blob.core.windows.net"
        endpoint_conf = f"spark.hadoop.fs.azure.account.endpoint.{AZURITE_ACCOUNT}.blob.core.windows.net"

        spark = SparkSession.builder \
            .appName("TimescaleToDelta") \
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-azure:3.3.4") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config(account_key_conf, AZURITE_KEY) \
            .config(endpoint_conf, AZURITE_BLOB_ENDPOINT) \
            .config("spark.hadoop.fs.azure.always.use.https", "false") \
            .config("spark.hadoop.fs.azure.secure.mode", "false") \
            .getOrCreate()
            
        logger.info("Spark Session created successfully.")
        return spark
    except Exception as e:
        logger.error(f"Failed to create Spark Session: {e}")
        sys.exit(1)

def extract_and_load(spark, table_name, run_timestamp):
    try:
        logger.info(f"Starting extraction for table: {table_name}")

        df = spark.read \
            .format("jdbc") \
            .option("url", DB_URL) \
            .option("dbtable", table_name) \
            .option("user", DB_USER) \
            .option("password", DB_PASSWORD) \
            .option("driver", DB_DRIVER) \
            .load()

        df_transformed = df.withColumn("ingestion_date", lit(run_timestamp))
        output_path = f"{WASBS_PATH}/{table_name}"

        df_transformed.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("ingestion_date") \
            .option("overwriteSchema", "true") \
            .save(output_path)

        logger.info(f"Successfully wrote {table_name} to {output_path}")

    except Exception as e:
        logger.error(f"Error processing table {table_name}: {e}")

def main():
    spark = create_spark_session()
    run_timestamp = datetime.now().strftime("%Y-%m-%d")
    
    for table in TABLES_TO_EXTRACT:
        extract_and_load(spark, table, run_timestamp)

    spark.stop()
    logger.info("Job finished.")

if __name__ == "__main__":
    main()