import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    jars = [
        "file:///opt/flink/lib/flink-connector-jdbc-3.1.2-1.18.jar",
        "file:///opt/flink/lib/postgresql-42.7.3.jar",
        "file:///opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar"
    ]
    
    jar_urls = ";".join(jars)
    env.add_jars(jars[0], jars[1], jars[2])
    

if __name__ == '__main__':
    run()