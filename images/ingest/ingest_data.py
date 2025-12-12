import random
import time
import threading
import json
import logging
import os
import sys
import datetime
from kafka import KafkaProducer, errors, KafkaAdminClient
from kafka.admin import NewTopic

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - [Machine-%(threadName)s] - %(message)s'
)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'redpanda:9092')
TOPIC_NAME = os.getenv('TOPIC_NAME', 'machine-readings')

SEND_INTERVAL = float(os.getenv('SEND_INTERVAL', '2.0'))
GENERATE_HISTORY = os.getenv('GENERATE_HISTORY', 'true').lower() == 'true'
HISTORY_DAYS = int(os.getenv('HISTORY_DAYS', '7')) 
HISTORY_SAMPLE_INTERVAL_MINUTES = 60 

class Sensor:
    def __init__(self, sensor_type, mu, sigma):
        self.type = sensor_type
        self.mu = mu
        self.sigma = sigma
        
    def generate(self):
        return random.gauss(self.mu, self.sigma)

class Machine(threading.Thread):
    def __init__(self, machine_id, machine_type, location, topic, producer):
        threading.Thread.__init__(self)
        self.id = machine_id
        self.type = machine_type
        self.location = location
        self.sensors = []
        self.topic = topic
        self.producer = producer
        self.daemon = True 

    def add_sensor(self, sensor):
        self.sensors.append(sensor)

    def create_payload(self, sensor, timestamp_obj):
        """
        Creates the dictionary payload.
        """
        return {
            "device_id": self.id,
            "device_type": self.type,
            "sensor_type": sensor.type,
            "sensor_val": round(sensor.generate(), 4), 
            "location": self.location,
            "time": timestamp_obj.isoformat()
        }

    def publish_reading(self, payload):
        """
        Handles the actual sending logic including Keying for Partitioning.
        """
        try:
            key_bytes = str(self.id).encode('utf-8')
            value_bytes = json.dumps(payload).encode('utf-8')

            self.producer.send(
                self.topic, 
                key=key_bytes, 
                value=value_bytes
            ).add_errback(self.on_send_error)

        except Exception as e:
            logger.error(f"Error publishing message: {e}")

    def generate_historical_data(self):
        """
        Generates data from the past week up to now.
        """
        logger.info(f"Generating {HISTORY_DAYS} days of history...")
        
        end_time = datetime.datetime.now()
        current_time = end_time - datetime.timedelta(days=HISTORY_DAYS)
        
        msg_count = 0
        
        while current_time < end_time:
            for sensor in self.sensors:
                payload = self.create_payload(sensor, current_time)
                self.publish_reading(payload)
                msg_count += 1
            
            current_time += datetime.timedelta(minutes=HISTORY_SAMPLE_INTERVAL_MINUTES)

        self.producer.flush()
        logger.info(f"Finished generating history. Sent {msg_count} messages.")

    def run(self):
        self.name = str(self.id)
        logger.info(f"Machine {self.id} started.")
        
        if GENERATE_HISTORY:
            self.generate_historical_data()

        logger.info(f"Starting real-time generation (Interval: {SEND_INTERVAL}s)")
        
        while True:
            try:
                current_time = datetime.datetime.now()
                for sensor in self.sensors:
                    payload = self.create_payload(sensor, current_time)
                    self.publish_reading(payload)
                    logger.debug(f"Sent: {payload}")
                
                time.sleep(SEND_INTERVAL)
                    
            except Exception as e:
                logger.error(f"Error in Machine execution loop: {e}")
                time.sleep(5)

    def on_send_error(self, exc):
        logger.error(f"Kafka Producer Error: {exc}")

def wait_for_kafka_and_setup():
    """
    Retries connecting to Kafka until successful and sets up topics.
    """
    admin_client = None
    
    while True:
        try:
            logger.info(f"Connecting to Admin Client at {BOOTSTRAP_SERVERS}...")
            admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
            
            topic = NewTopic(name=TOPIC_NAME, num_partitions=3)
            
            try:
                admin_client.create_topics(new_topics=[topic], validate_only=False)
                logger.info(f"Topic '{TOPIC_NAME}' created.")
            except errors.TopicAlreadyExistsError:
                logger.info(f"Topic '{TOPIC_NAME}' already exists.")
            finally:
                admin_client.close()
            break
            
        except errors.NoBrokersAvailable:
            logger.warning("No Brokers Available. Retrying in 5 seconds...")
            time.sleep(5)
        except Exception as e:
            logger.error(f"Connection error: {e}. Retrying in 5 seconds...")
            time.sleep(5)

    logger.info("Initializing Producer...")
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        acks='all', 
        retries=5,
        compression_type='gzip',
        batch_size=16384,
        linger_ms=10
    )
    return producer

def main():
    producer = wait_for_kafka_and_setup()

    sensor_types = [
        Sensor("temperature", 65, 5),  
        Sensor("vibration", 0.4, 0.1),
        Sensor("pressure", 100, 10)
    ]

    machines = []
    machine_configs = [
        (0, "Press", "Factory Floor 1"),
        (1, "Drill", "Factory Floor 1"),
        (2, "Conveyor", "Factory Floor 2")
    ]

    for m_id, m_type, m_loc in machine_configs:
        m = Machine(m_id, m_type, m_loc, TOPIC_NAME, producer)
        for s in sensor_types:
            m.add_sensor(s)
        machines.append(m)

    for m in machines:
        m.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping application...")
        producer.close()
        sys.exit(0)

if __name__ == "__main__":
    main()