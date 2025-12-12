# DE

https://medium.com/data-science/how-i-dockerized-apache-flink-kafka-and-postgresql-for-real-time-data-streaming-c4ce38598336

docker-compose exec flink-jobmanager flink run -py /opt/flink/usr_jobs/sensor_aggregation.py


docker exec -it timescaledb psql -U admin -d iiot

SELECT * FROM machine_sensors ORDER BY time DESC LIMIT 10 ;
SELECT * FROM sensor_aggregates ORDER BY window_start DESC LIMIT 10 ;

TIMESCALE_CONN_STRING="postgresql://admin:admin@localhost:5432/iiot"
AZURITE_CONN_STRING="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"


prefect deploy prefect-flows/nightly_etl_flow.py:nightly_etl_flow --name "Nightly ETL Deployment" --cron "0 2 * * *" --timezone "UTC" --work-queue "production-etl" --tag "etl"  --tag "nightly" --tag "spark" --tag "production"

spark-submit --packages "io.delta:delta-spark_2.13:4.0.0,org.apache.hadoop:hadoop-azure:3.4.0,com.microsoft.azure:azure-storage:8.6.6,org.postgresql:postgresql:42.7.8" --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" .\spark-jobs\timescale_to_deltalake.py