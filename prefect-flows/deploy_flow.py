# prefect-flows/deploy_flow.py

from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule

# Import the flow from the other file
from nightly_etl_flow import nightly_etl_flow

def build_and_apply_deployment():
    """
    Builds and applies the Prefect deployment for the nightly ETL flow.
    """
    # Create the deployment object
    deployment = Deployment.build_from_flow(
        flow=nightly_etl_flow,
        name="Nightly ETL Deployment",
        
        # Schedule to run at 2:00 AM UTC every night
        schedule=(CronSchedule(cron="0 2 * * *", timezone="UTC")),
        
        # Specify a work queue for your agent to pull from
        work_queue_name="production-etl",
        
        # Add tags for better organization and filtering in the UI
        tags=["etl", "nightly", "spark", "production"],
    )

    # The `apply` method registers the deployment with the Prefect API
    deployment.apply()
    print("Deployment for 'nightly_etl_flow' has been successfully created and applied.")
    print("Schedule: Every night at 2:00 AM UTC.")
    print("Work Queue: 'production-etl'")


if __name__ == "__main__":
    build_and_apply_deployment()