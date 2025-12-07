#!/usr/bin/env python3
"""
Simple PyFlink job that reads sensor JSON from Redpanda/Kafka
and prints the parsed records.
Tested with Flink 1.18–1.20 + Redpanda
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
    KafkaTopicPartition,
)
from pyflink.common.serialization import DeserializationSchema
from pyflink.common.typeinfo import Types
from pyflink.common import Row
import json
import os


# ------------------------------------------------------------------
# 1. Custom JSON deserializer (bytes → Row)
# ------------------------------------------------------------------
class SensorJsonDeserializationSchema(DeserializationSchema[Row]):
    def deserialize(self, message: bytes) -> Row:
        if message is None:
            return None

        txt = message.decode("utf-8")
        data = json.loads(txt)

        return Row(
            device_id=int(data["device_id"]),
            device_type=str(data["device_type"]),
            sensor_type=str(data["sensor_type"]),
            sensor_val=float(data["sensor_val"]),
            location=str(data["location"]),
            timestamp=float(data["time"]),   # renamed to timestamp for clarity
        )

    def is_end_of_stream(self, next_element: Row) -> bool:
        return False

    def get_produced_type(self):
        return Types.ROW_NAMED(
            field_names=[
                "device_id",
                "device_type",
                "sensor_type",
                "sensor_val",
                "location",
                "timestamp",
            ],
            field_types=[
                Types.INT(),
                Types.STRING(),
                Types.STRING(),
                Types.DOUBLE(),
                Types.STRING(),
                Types.DOUBLE(),
            ],
        )


# ------------------------------------------------------------------
# 2. Main job
# ------------------------------------------------------------------
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # increase later if you want

    # ---- Change these values for your environment ----
    BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "redpanda:9092")  # default for local Redpanda
    TOPIC_NAME        = os.getenv("KAFKA_TOPIC", "sensor-data")       # change if needed
    CONSUMER_GROUP    = "flink-sensor-test-group"
    # --------------------------------------------------

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_topics(TOPIC_NAME)
        .set_group_id(CONSUMER_GROUP)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())  # or .latest()
        .set_value_only_deserializer(SensorJsonDeserializationSchema())
        .build()
    )

    stream = env.from_source(
        source,
        "Redpanda-Sensor-Source"
    )

    # Simple processing: just print
    stream.print()

    # You can add more here later, e.g.:
    #   .map(lambda r: f"{r.device_id} -> {r.sensor_val}")
    #   .sink_to(...)

    env.execute("Redpanda → Flink Sensor Test Job")


if __name__ == "__main__":
    main()