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


class RedpandaParser:
    @staticmethod
    def get_timescale_config():
        raw_data_sql = (
            "INSERT INTO machine_sensors (time, device_id, device_type, sensor_type, sensor_val, location)"
            "VALUES (?, ?, ?, ?, ?, ?)"
        )
        raw_sql_types = Types.ROW([
                Types.SQL_TIMESTAMP(),  # time
                Types.INT(),            # device_id
                Types.STRING(),         # device_type
                Types.STRING(),         # sensor_type
                Types.DOUBLE(),         # sensor_val
                Types.STRING(),         # location
            ])
        return (raw_data_sql, raw_sql_types)
    
    @staticmethod
    def parse_data(data: str) -> Row:
        data = json.loads(data)
        device_id = int(data["device_id"])
        device_type = data["device_type"]
        sensor_type = data["sensor_type"]
        sensor_val = float(data["sensor_val"])
        location = data["location"]
        time = datetime.strptime(data["time"], "%Y-%m-%dT%H:%M:%S.%f")
        return Row(time, device_id, device_type, sensor_type, sensor_val, location)

def initialize_env() -> StreamExecutionEnvironment:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    jars = [
        "file:///opt/flink/lib/flink-connector-jdbc-3.1.2-1.18.jar",
        "file:///opt/flink/lib/postgresql-42.7.3.jar",
        "file:///opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar"
    ]
    
    env.add_jars(jars[0], jars[1], jars[2])
    return env
    
def configure_redpanda_source(server: str) -> KafkaSource:
    properties = {
        "bootstrap.servers": server,
        "group.id": "flink-consumer",
    }

    offset = KafkaOffsetsInitializer.latest()
    kafka_source = (
        KafkaSource.builder()
        .set_topics(REDPANDA_TOPIC)
        .set_properties(properties)
        .set_starting_offsets(offset)
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    return kafka_source

def configure_timescale_sink(sql_dml: str, type_info: Types) -> JdbcSink:
    return JdbcSink.sink(
        sql_dml,
        type_info,
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .with_url(f"jdbc:postgresql://{TIMESCALE_HOST}/{TIMESCALE_DB}")
        .with_driver_name("org.postgresql.Driver")
        .with_user_name("admin")
        .with_password("admin")
        .build(),
        JdbcExecutionOptions.builder()
        .with_batch_interval_ms(1000)
        .with_batch_size(200)
        .with_max_retries(5)
        .build(),
    )

class FirstElementTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return int(value[0].timestamp() * 1000)

class Aggregator(AggregateFunction):
    def create_accumulator(self) -> Tuple[float, float, float, int]:
        # total_val, min_val, max_val, count
        return 0.0, 0.0, 0.0, 0

    def add(self, value: Tuple[str, int], accumulator: Tuple[float, float, float, int]) -> Tuple[float, float, float, int]:
        return (
            accumulator[0] + value[4],
            min(accumulator[1], value[4]),
            max(accumulator[2], value[4]),
            accumulator[3] + 1,
        )
    
    def get_result(self, accumulator: Tuple[float, float, float, int]) -> Tuple[float, float, float, int]:
        return (
            accumulator[0] / accumulator[3] if accumulator[3] != 0 else 0,
            accumulator[1],
            accumulator[2],
            accumulator[3]
        )

    def merge(self, a: Tuple[float, float, float, int], b: Tuple[float, float, float, int]) -> Tuple[float, float, float, int]:
        return (
            a[0] + b[0],
            min(a[1], b[1]),
            max(a[2], b[2]),
            a[3] + b[3]
        )

class WindowFunction(ProcessWindowFunction):
    def __init__(self, window_type: str):
        super().__init__()
        self.window_type = window_type
    
    @staticmethod
    def get_timescale_config():
        agg_data_sql = (
        "INSERT INTO sensor_aggregates (window_start, window_type, device_id, device_type, sensor_type, average_val, min_val, max_val, count)"
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        agg_data_types = Types.ROW([
            Types.SQL_TIMESTAMP(),  # window_start
            Types.STRING(),         # window_type
            Types.INT(),            # device_id
            Types.STRING(),         # device_type
            Types.STRING(),         # sensor_type
            Types.DOUBLE(),         # average_val
            Types.DOUBLE(),         # min_val
            Types.DOUBLE(),         # max_val
            Types.INT()             # count
            ])
        return (agg_data_sql, agg_data_types) 

    def process(self, key, context, elements):
        device_id, device_type, sensor_type = key

        agg = next(iter(elements))
        avg, min, max, count = agg

        window_start = datetime.fromtimestamp(context.window().start / 1000)

        yield Row(
            window_start,
            self.window_type,
            device_id,
            device_type,
            sensor_type,
            avg,
            min,
            max,
            count
        )


def main():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())
    
    env = initialize_env()
    logger.info("Initialized environment")
    
    redpanda_source = configure_redpanda_source(REDPANDA_HOST)
    logger.info(f"Created Redpanda source from {REDPANDA_HOST}")
    
    
    raw_data_sink = configure_timescale_sink(*RedpandaParser.get_timescale_config())
    logger.info(f"Created TimescaleDB sink {TIMESCALE_HOST}/{TIMESCALE_DB}/machine_sensors")

    agg_data_sink = configure_timescale_sink(*WindowFunction.get_timescale_config())
    logger.info(f"Created TimescaleDB sink {TIMESCALE_HOST}/{TIMESCALE_DB}/sensor_aggregates")

    
    ds_raw = env.from_source(
        redpanda_source, WatermarkStrategy.no_watermarks(), "Redpanda machine-readings topic"
    )
    logger.info(f"Created raw datastream")
    
    ds_transformed = ds_raw \
        .map(RedpandaParser.parse_data, output_type=RedpandaParser.get_timescale_config()[1]) \
        .assign_timestamps_and_watermarks(
            WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5))
            .with_timestamp_assigner(FirstElementTimestampAssigner())
        )
    logger.info(f"Defined datastream with 5s watermark and json parsing")
    
    ds_tumble = ds_transformed \
        .key_by(lambda row: (row[1], row[2], row[3])) \
        .window(TumblingEventTimeWindows.of(Time.seconds(60))) \
        .aggregate(Aggregator(),
                    window_function=WindowFunction("tumbling"),
                    output_type=WindowFunction.get_timescale_config()[1])
    logger.info("Defined tumbling window agg over 60s with datastream API")

    ds_sliding = ds_transformed \
        .key_by(lambda row: (row[1], row[2], row[3])) \
        .window(SlidingEventTimeWindows.of(Time.seconds(60), Time.seconds(10))) \
        .aggregate(Aggregator(),
                    window_function=WindowFunction("sliding"),
                    output_type=WindowFunction.get_timescale_config()[1])
    logger.info("Defined tumbling window agg over 60s with datastream API")


    ds_tumble.print()
    ds_tumble.add_sink(agg_data_sink)
    logger.info("Sinking agg data (tumbling) to timescale")

    ds_sliding.print()
    ds_sliding.add_sink(agg_data_sink)
    logger.info("Sinking agg data (silding) to timescale")

    ds_transformed.add_sink(raw_data_sink)
    logger.info("Sinking raw parsed data to timescale")
    
    env.execute("flink-data-ingestion-job")

if __name__ == '__main__':
    main()