# STANDARD MODULES
import os
import time
import json
from kafka import KafkaClient
from kafka import KafkaProducer
from kafka.errors import KafkaError


def get_localtime():
    """
        Returns current date and time
    """
    localtime = time.strftime("%A %B %d %Y %I:%M:%S %p %Z", time.localtime())
    return localtime


class Producer(object):
    """
        Producer class used to insert message in kafka messaging queue
    """

    def __init__(self, server, port):
        """
            Constructor: Initialise Kafka Server
        """
        kafka_server = "{}:{}".format(server, port)
        self.client = KafkaClient(kafka_server)
        self.producer = KafkaProducer(bootstrap_servers=kafka_server)

    def put(self, topic, msg):
        """
            Post message to a queue
        """
        future = self.producer.send(topic, json.dumps(msg).encode('utf-8'))
        # Block for 'synchronous' sends
        try:
            print("\nMessage to kafka queue: {} stored successfully at {}".format(topic, get_localtime()))
            print(">>> {}\n".format(msg))
            record_metadata = future.get(timeout=10)
            print("-: Metadata :-")
            print("Topic = ", record_metadata.topic)
            print("Partition = ", record_metadata.partition)
            print("Offset = ", record_metadata.offset)
            print("\n")
        except KafkaError:
            # Decide what to do if produce request failed...
            message = "\nLooks like there is some problem while sending a message in KAFKA\n"
            print(message)

    def __del__(self):
        """
            Destructor: Closing the producer object
        """
        self.producer.close()
