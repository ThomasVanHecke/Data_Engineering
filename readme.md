# DE

https://medium.com/data-science/how-i-dockerized-apache-flink-kafka-and-postgresql-for-real-time-data-streaming-c4ce38598336

docker-compose exec flink-jobmanager flink run -py /opt/flink/usr_jobs/sensor_aggregation.py


docker exec -it timescaledb psql -U admin -d iiot

SELECT * FROM machine_sensors ORDER BY time DESC LIMIT 10 ;
SELECT * FROM sensor_aggregates ORDER BY window_start DESC LIMIT 10 ;

TIMESCALE_CONN_STRING="postgresql://admin:admin@localhost:5432/iiot"
AZURITE_CONN_STRING="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"


prefect deploy prefect-flows/nightly_etl_flow.py:nightly_etl_flow --name "Nightly ETL Deployment" --cron "0 2 * * *" --timezone "UTC" --work-queue "production-etl" --tag "etl"  --tag "nightly" --tag "spark" --tag "production"
