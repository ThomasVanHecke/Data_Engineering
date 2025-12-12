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

# TODO: transform data (T)
# TODO: load data (L)
AZURITE_ACCOUNT_NAME = "devstoreaccount1"
AZURITE_ACCOUNT_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
spark.conf.set(
    "fs.azure.account.key.{}.blob.core.windows.net".format(AZURITE_ACCOUNT_NAME),
    AZURITE_ACCOUNT_KEY
)
# custom endpoint configuration
spark.conf.set(
    "fs.azure.endpoint.{}.blob.core.windows.net".format(AZURITE_ACCOUNT_NAME),
    "http://azurite:10000"
)
container_name = "datalake"
table_path = "my-delta-table"
azurite_path = "wasb://{}@{}.blob.core.windows.net/{}".format(
    container_name, 
    AZURITE_ACCOUNT_NAME, 
    table_path
)
    # TODO: partition on timestamp
df.write.format("delta").mode("overwrite").partitionBy("time").save(azurite_path)
"""# TODO: update requirements.txt"""