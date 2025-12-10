# prefect-flows/nightly_etl_flow.py

import os
import random
import time
from datetime import timedelta

import psycopg2
from azure.core.exceptions import ServiceRequestError
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from prefect import flow, get_run_logger, task
from prefect.tasks import task_input_hash

# Load environment variables from .env file
load_dotenv()

# --- Task 1: Check TimescaleDB Connection ---
@task(
    name="Check TimescaleDB Connection",
    retries=3,
    retry_delay_seconds=20,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(minutes=30),
    description="Verifies connection to the TimescaleDB."
)
def check_timescaledb_connection():
    """
    Connects to TimescaleDB to ensure it is available.
    Retries 3 times with a 20-second delay on failure.
    """
    logger = get_run_logger()
    logger.info("Attempting to connect to TimescaleDB...")
    
    conn_str = os.getenv("TIMESCALE_CONN_STRING")
    if not conn_str:
        raise ValueError("TIMESCALE_CONN_STRING environment variable not set.")

    try:
        with psycopg2.connect(conn_str) as conn:
            logger.info("Successfully connected to TimescaleDB.")
        return True
    except psycopg2.OperationalError as e:
        logger.error(f"Failed to connect to TimescaleDB: {e}")
        # Re-raise the exception to trigger Prefect's retry mechanism
        raise

# --- Task 2: Check Azurite Connection ---
@task(
    name="Check Azurite Connection",
    retries=2,
    retry_delay_seconds=10,
    description="Verifies connection to the Azurite Blob Storage."
)
def check_azurite_connection():
    """
    Connects to Azurite to ensure it is available.
    Retries 2 times with a 10-second delay on failure.
    """
    logger = get_run_logger()
    logger.info("Attempting to connect to Azurite Blob Storage...")
    
    conn_str = os.getenv("AZURITE_CONN_STRING")
    if not conn_str:
        raise ValueError("AZURITE_CONN_STRING environment variable not set.")

    try:
        blob_service_client = BlobServiceClient.from_connection_string(conn_str)
        # A simple operation to confirm connectivity
        blob_service_client.get_service_properties()
        logger.info("Successfully connected to Azurite.")
        return True
    except ServiceRequestError as e:
        logger.error(f"Failed to connect to Azurite: {e}")
        # Re-raise to trigger retry
        raise

# --- Task 3: Submit Spark Job ---
@task(
    name="Submit Spark Job",
    retries=2,
    retry_delay_seconds=30,
    description="Submits the main data processing job to Spark."
)
def submit_spark_job():
    """
    Placeholder task to simulate submitting a Spark batch job.
    In a real scenario, this would use an API or CLI to submit the job.
    """
    logger = get_run_logger()
    job_duration = random.randint(45, 120)  # Simulate a job taking 45-120 seconds
    job_id = f"spark-job-{random.randint(1000, 9999)}"
    
    logger.info(f"Submitting Spark job '{job_id}'. Estimated duration: {job_duration} seconds.")

    # --- PLACEHOLDER ---
    # TODO: Replace this sleep with your actual Spark job submission logic.
    # For example, using `spark-submit`, a Databricks API call, or a Kubernetes job.
    time.sleep(job_duration)
    # --- END PLACEHOLDER ---
    
    # Simulate a potential random failure
    if random.random() < 0.1: # 10% chance of failure
        logger.error(f"Spark job '{job_id}' failed during execution.")
        raise RuntimeError("Simulated Spark job failure.")

    logger.info(f"Spark job '{job_id}' completed successfully.")
    return job_id

# --- Task 4: Verify Delta Lake Data ---
@task(
    name="Verify Delta Lake Data",
    description="Checks the output data in Delta Lake for correctness and record count."
)
def verify_delta_lake_data(spark_job_id: str):
    """
    Placeholder task to simulate verifying data written to a Delta Lake table.
    """
    logger = get_run_logger()
    logger.info(f"Verifying Delta Lake data produced by job '{spark_job_id}'...")

    # --- PLACEHOLDER ---
    # TODO: Replace this with actual data verification logic.
    # This might involve a Spark query to count records, check for nulls, etc.
    # from pyspark.sql import SparkSession
    # spark = SparkSession.builder.appName("Verification").getOrCreate()
    # df = spark.read.format("delta").load("path/to/your/delta_table")
    # record_count = df.count()
    time.sleep(10) # Simulate query time
    record_count = random.randint(50000, 250000)
    # --- END PLACEHOLDER ---

    logger.info(f"Verification successful. Found {record_count:,} new records in Delta Lake.")
    return record_count


# --- The Main Flow ---
@flow(name="Nightly ETL Pipeline")
def nightly_etl_flow():
    """
    The main ETL flow orchestrating all tasks for the nightly data processing.
    """
    logger = get_run_logger()
    logger.info("--- Starting Nightly ETL Flow ---")

    # The connection checks can run in parallel
    ts_ready = check_timescaledb_connection.submit()
    azurite_ready = check_azurite_connection.submit()

    # The Spark job depends on both connections being ready
    job_id = submit_spark_job.submit(wait_for=[ts_ready, azurite_ready])

    # Verification depends on the Spark job finishing
    final_record_count = verify_delta_lake_data.submit(spark_job_id=job_id)

    # Summarize the results
    # .result() will block until the task is complete
    count = final_record_count.result()
    logger.info("--- Nightly ETL Flow Summary ---")
    logger.info(f"✅ Flow completed successfully.")
    logger.info(f"📊 Total records processed and verified: {count:,}")
    logger.info("---------------------------------")


if __name__ == "__main__":
    # This allows you to run the flow manually for testing
    nightly_etl_flow()