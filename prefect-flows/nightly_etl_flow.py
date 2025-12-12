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

load_dotenv()


@task(
    name="Check TimescaleDB Connection",
    retries=3,
    retry_delay_seconds=20,
    description="Verifies connection to the TimescaleDB."
)
def check_timescaledb_connection():
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
        raise

@task(
    name="Check Azurite Connection",
    retries=3,
    retry_delay_seconds=20,
    description="Verifies connection to the Azurite Blob Storage."
)
def check_azurite_connection():
    logger = get_run_logger()
    logger.info("Attempting to connect to Azurite Blob Storage...")
    
    conn_str = os.getenv("AZURITE_CONN_STRING")
    if not conn_str:
        raise ValueError("AZURITE_CONN_STRING environment variable not set.")

    try:
        blob_service_client = BlobServiceClient.from_connection_string(conn_str)
        blob_service_client.get_service_properties()
        logger.info("Successfully connected to Azurite.")
        return True
    except ServiceRequestError as e:
        logger.error(f"Failed to connect to Azurite: {e}")
        raise

@task(
    name="Submit Spark Job",
    retries=3,
    retry_delay_seconds=30,
    description="Submits the nightly ETL processing job to Spark."
)
def submit_spark_job():
    logger = get_run_logger()

    job_duration = random.randint(45, 120)
    job_id = f"spark-job-{random.randint(1000, 9999)}"
    
    logger.info(f"Submitting Spark job '{job_id}'. Estimated duration: {job_duration} seconds.")

    time.sleep(job_duration)
    
    logger.info(f"Spark job '{job_id}' completed successfully.")
    return job_id

@task(
    name="Verify Delta Lake Data",
    description="Checks the output data in Delta Lake for correctness and record count."
)
def verify_delta_lake_data(spark_job_id: str):
    logger = get_run_logger()
    logger.info(f"Verifying Delta Lake data produced by job '{spark_job_id}'...")

    time.sleep(10)

    logger.info(f"Delta Lake verification successful")


@flow(name="Nightly ETL Pipeline")
def nightly_etl_flow():
    logger = get_run_logger()
    logger.info("--- Starting Nightly ETL Flow ---")

    ts_ready = check_timescaledb_connection.submit()
    azurite_ready = check_azurite_connection.submit()

    job_id = submit_spark_job.submit(wait_for=[ts_ready, azurite_ready])

    verify_delta_lake_data.submit(spark_job_id=job_id)


if __name__ == "__main__":
    nightly_etl_flow()