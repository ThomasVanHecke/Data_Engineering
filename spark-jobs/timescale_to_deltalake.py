from pyspark.sql import SparkSession
# TODO: jdbc connection to timescale
url = "jdbc:postgresql://localhost:5432/iiot"
properties = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}
spark = SparkSession.builder \
    .appName("myapp") \
    .getOrCreate()
# TODO: extract data (E)
df = spark.read.jdbc(url=url, table="machine_sensors", properties=properties)
df.show()
"""# TODO: transform data (T)
    # TODO: create delta table
# TODO: load data (L)
container_name = "datalake"
AZURITE_ACCOUNT_NAME = "devstoreaccount1"
table_path = "test1"
azurite_path = "wasbs://{}@{}.blob.core.windows.net/{}".format(
    container_name, 
    AZURITE_ACCOUNT_NAME, 
    table_path
)
    # TODO: partition on timestamp
df.write.format("delta").partitionBy("time").save(azurite_path)
# TODO: update requirements.txt"""