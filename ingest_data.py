import random
from time import sleep
import threading
import json
from kafka import KafkaProducer, errors, KafkaAdminClient
from kafka.admin import NewTopic
import datetime

# TODO: create class (Sensor)
class Sensor:
    # TODO: constructor
    def __init__(self, type, mu, sigma) -> None:
        self.type = type
        self.mu = mu
        self.sigma = sigma
        
    # TODO: generate (method)
    def generate(self):
        return random.gauss(self.mu, self.sigma)

# TODO: create class (Machine)
class Machine(threading.Thread):
    # TODO: instance variables
    def __init__(self, id, type, location, topic, producer):
        threading.Thread.__init__(self)
        self.id = id
        self.type = type
        self.location = location
        self.sensors : list[Sensor] = []
        self.topic = topic
        self.producer = producer

    def add_sensor(self, sensor):
        self.sensors.append(sensor)

    def create_json(self, sensor_value, sensor_type, timestamp):
        data = {
            "device_id" : self.id,
            "device_type" : self.type,
            "sensor_type" : sensor_type,
            "sensor_val" : sensor_value,
            "location" : self.location,
            "time" : timestamp
        }
        return json.dumps(data)

    def run(self):
        while True:
            for sensor in self.sensors:
                sensor_value = sensor.generate()
                json_string = self.create_json(sensor_value, sensor.type, datetime.datetime.now().timestamp())
                self.producer.send(self.topic, json_string.encode("utf-8"))
                print(json_string)
                sleep(2)

def main():
    try:
        admin_client = KafkaAdminClient(bootstrap_servers = 'localhost:19092')
        # TODO: decision: partitions, replication factors
        topic = NewTopic(name="machine-sensor", num_partitions=1, replication_factor=1)
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        admin_client.close()
    except errors.TopicAlreadyExistsError:
        pass
    producer = KafkaProducer(
        bootstrap_servers = "localhost:19092"
    )

    topic = "machine-readings"

    # TODO: create class instance
    sensor1 = Sensor("sensor 1", 30, 3)
    sensor2 = Sensor("sensor 2", 30, 3)
    sensor3 = Sensor("sensor 3", 30, 3)
    machine1 = Machine(0, "some type", "some location", topic, producer)
    machine2 = Machine(1, "some type", "some location", topic, producer)
    machine3 = Machine(2, "some type", "some location", topic, producer)
    # TODO: call instance method
    machine1.add_sensor(sensor1)
    machine1.add_sensor(sensor2)
    machine1.add_sensor(sensor3)

    machine2.add_sensor(sensor1)
    machine2.add_sensor(sensor2)
    machine2.add_sensor(sensor3)

    machine3.add_sensor(sensor1)
    machine3.add_sensor(sensor2)
    machine3.add_sensor(sensor3)
    # TODO: start threads
    machine1.start()
    machine2.start()
    machine3.start()


if __name__ == "__main__":
    main()