import random
import time
import threading
import json
import logging
import os
import sys
from kafka import KafkaProducer, errors, KafkaAdminClient
from kafka.admin import NewTopic
import datetime

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [Machine-%(threadName)s] - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration via Environment Variables (Best for Docker)
BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'redpanda:9092')
TOPIC_NAME = os.getenv('TOPIC_NAME', 'machine-readings')

class Sensor:
    def __init__(self, sensor_type, mu, sigma) -> None:
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
        self.sensors: list[Sensor] = []
        self.topic = topic
        self.producer = producer
        # Daemon threads exit when the main program exits
        self.daemon = True 

    def add_sensor(self, sensor):
        self.sensors.append(sensor)

    def create_json(self, sensor_value, sensor_type, timestamp):
        data = {
            "device_id": self.id,
            "device_type": self.type,
            "sensor_type": sensor_type,
            "sensor_val": sensor_value,
            "location": self.location,
            "time": timestamp
        }
        return json.dumps(data)

    def run(self):
        # Rename thread for better logging
        self.name = str(self.id)
        logger.info(f"Machine {self.id} started.")
        
        while True:
            try:
                for sensor in self.sensors:
                    sensor_value = sensor.generate()
                    timestamp = datetime.datetime.now().isoformat()
                    json_string = self.create_json(sensor_value, sensor.type, timestamp)
                    
                    # Async send with callback for error handling on the producer side
                    self.producer.send(self.topic, json_string.encode("utf-8")).add_errback(self.on_send_error)
                    
                    logger.info(f"Sent: {json_string}")
                    time.sleep(2) # Simulate delay between sensors
                    
            except Exception as e:
                # Catch logic errors or thread-level crashes, log them, and restart loop
                logger.error(f"Error in Machine execution loop: {e}")
                logger.info("Restarting Machine loop in 5 seconds...")
                time.sleep(5)

    def on_send_error(self, exc):
        logger.error(f"Kafka Producer Error: {exc}")

def wait_for_kafka_and_setup():
    """
    Retries connecting to Kafka until successful.
    Returns the Producer instance.
    """
    admin_client = None
    producer = None
    
    while True:
        try:
            logger.info(f"Attempting to connect to Kafka at {BOOTSTRAP_SERVERS}...")
            
            # 1. Setup Admin Client to create Topic
            admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
            topic = NewTopic(name=TOPIC_NAME, num_partitions=1, replication_factor=1)
            
            try:
                admin_client.create_topics(new_topics=[topic], validate_only=False)
                logger.info(f"Topic '{TOPIC_NAME}' created.")
            except errors.TopicAlreadyExistsError:
                logger.info(f"Topic '{TOPIC_NAME}' already exists.")
            finally:
                admin_client.close()

            # 2. Setup Producer
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                # Retries internally within the library
                retries=5,
                value_serializer=lambda v: v # Already encoded in Machine class, or remove encode there and put json.dumps here
            )
            logger.info("Successfully connected to Kafka.")
            return producer

        except errors.NoBrokersAvailable:
            logger.warning("No Brokers Available. Retrying in 5 seconds...")
            time.sleep(5)
        except Exception as e:
            logger.error(f"Unexpected connection error: {e}. Retrying in 5 seconds...")
            time.sleep(5)

def main():
    # 1. robust initialization
    producer = wait_for_kafka_and_setup()

    # 2. Setup Sensors
    sensor1 = Sensor("temperature", 30, 3)
    sensor2 = Sensor("vibration", 5, 0.5)
    sensor3 = Sensor("pressure", 100, 10)

    # 3. Setup Machines
    machines = []
    
    m1 = Machine(0, "Press", "Factory Floor 1", TOPIC_NAME, producer)
    m1.add_sensor(sensor1)
    m1.add_sensor(sensor2)
    m1.add_sensor(sensor3)
    machines.append(m1)

    m2 = Machine(1, "Drill", "Factory Floor 1", TOPIC_NAME, producer)
    m2.add_sensor(sensor1)
    m2.add_sensor(sensor2)
    m2.add_sensor(sensor3)
    machines.append(m2)

    m3 = Machine(2, "Conveyor", "Factory Floor 2", TOPIC_NAME, producer)
    m3.add_sensor(sensor1)
    m3.add_sensor(sensor2)
    m3.add_sensor(sensor3)
    machines.append(m3)

    # 4. Start Threads
    for m in machines:
        m.start()

    # 5. Keep main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping application...")
        producer.close()
        sys.exit(0)

if __name__ == "__main__":
    main()