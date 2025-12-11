import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.datastream.functions import AggregateFunction, ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows, SlidingEventTimeWindows
from pyflink.table.window import Slide
from pyflink.table import DataTypes, Schema
from pyflink.common import Row, Types, WatermarkStrategy, Duration
from pyflink.common import Time
from pyflink.datastream.connectors import JdbcSink
from pyflink.datastream.connectors.jdbc import (
    JdbcConnectionOptions,
    JdbcExecutionOptions,
)
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from typing import Tuple
from pyflink.table.expressions import col, lit
import time
from datetime import datetime
import json
import logging

REDPANDA_HOST = "redpanda:9092"
REDPANDA_TOPIC = "machine-readings"
TIMESCALE_HOST = "timescaledb:5432"
TIMESCALE_DB = "iiot"

def initialize_env():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    jars = [
        "file:///opt/flink/lib/flink-connector-jdbc-3.1.2-1.18.jar",
        "file:///opt/flink/lib/postgresql-42.7.3.jar",
        "file:///opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar"
    ]
    
    env.add_jars(jars[0], jars[1], jars[2])

    t_env = StreamTableEnvironment.create(env)

    return env, t_env

if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())

    env, t_env = initialize_env()

    t_env.execute_sql(f"""
        CREATE TABLE machine_readings (
            device_id INT,
            device_type STRING,
            sensor_type STRING,
            sensor_val DOUBLE,
            location STRING,
            `time` TIMESTAMP(3),
            WATERMARK FOR `time` AS `time` - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{REDPANDA_TOPIC}',
            'properties.bootstrap.servers' = '{REDPANDA_HOST}',
            'properties.group.id' = 'flink-table-consumer',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601'
        )
    """)
    logger.info("Created Table API Kafka source")

    debug_result = t_env.sql_query("""
        SELECT 
            device_id,
            sensor_type,
            sensor_val,
            `time`,
            CURRENT_WATERMARK(`time`) as current_watermark
        FROM machine_readings
    """)

    t_env.to_changelog_stream(debug_result).print()
    
    t_env.execute_sql(f"""
        CREATE TABLE sensor_aggregates_sink (
            window_start TIMESTAMP(3),
            window_type STRING,
            device_id INT,
            device_type STRING,
            sensor_type STRING,
            average_val DOUBLE,
            min_val DOUBLE,
            max_val DOUBLE,
            `count` INT,
            PRIMARY KEY (window_start, device_id, device_type, sensor_type, window_type) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://{TIMESCALE_HOST}/{TIMESCALE_DB}',
            'table-name' = 'sensor_aggregates',
            'username' = 'admin',
            'password' = 'admin',
            'driver' = 'org.postgresql.Driver'
        )
    """)
    logger.info("Created Table API JDBC sink for sensor_aggregates")

    tumbling_result = t_env.sql_query("""
        SELECT 
            TUMBLE_START(PROCTIME(), INTERVAL '60' SECOND) as window_start,
            'tumbling' as window_type,
            device_id,
            device_type,
            sensor_type,
            AVG(sensor_val) as average_val,
            MIN(sensor_val) as min_val,
            MAX(sensor_val) as max_val,
            CAST(COUNT(*) AS INT) as `count`
        FROM machine_readings
        GROUP BY 
            TUMBLE(PROCTIME(), INTERVAL '60' SECOND),
            device_id,
            device_type,
            sensor_type
    """)
    logger.info("Defined tumbling window agg over 60s with Table API (SQL)")

    t_env.execute_sql(f"""
        CREATE TABLE print_table WITH ('connector' = 'print')
        LIKE sensor_aggregates_sink (EXCLUDING ALL)
    """)

    stmt_set = t_env.create_statement_set()
    stmt_set.add_insert("sensor_aggregates_sink", tumbling_result)
    stmt_set.add_insert("print_table", tumbling_result)

    table_result = stmt_set.execute()
    logger.info("Job submitted, waiting for completion...")
    table_result.wait()