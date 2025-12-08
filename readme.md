# DE

https://medium.com/data-science/how-i-dockerized-apache-flink-kafka-and-postgresql-for-real-time-data-streaming-c4ce38598336

docker-compose exec flink-jobmanager flink run -py /opt/flink/usr_jobs/sensor_aggregation.py


docker exec -it timescaledb psql -U admin -d iiot

SELECT * FROM machine_sensors ORDER BY time DESC LIMIT 10 ;
SELECT * FROM sensor_aggregates ORDER BY window_start DESC LIMIT 10 ;