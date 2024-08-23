import asyncio
import threading
import os

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from core.logger import Logger
from core.consumers import worker, logger
from core.producers import send_messages

# Global variables
NUM_OF_CONSUMERS = 2
NUM_OF_PRODUCERS = 3
ENDLESS_MESSAGES = False


def create_producer() -> KafkaProducer:
    """
    Explanation:
        Creates ONE producer. The reason why is because the actual message production
        comes from send_messages(), found in assign_producers().
    """
    return KafkaProducer(
        bootstrap_servers="localhost:9092",
        security_protocol="SSL",
        ssl_check_hostname=False,
        ssl_cafile=os.path.join(os.path.dirname(__file__), "secrets/ca.pem"),
        ssl_certfile=os.path.join(os.path.dirname(__file__), "secrets/client-cert.pem"),
        ssl_keyfile=os.path.join(os.path.dirname(__file__), "secrets/client-key.pem"),
        ssl_password="password",
    )


async def assign_producers(
    producer: KafkaProducer, num_of_producers: int, endless_messages: bool
) -> list[asyncio.Task]:
    """
    Explanation:
        Creates asynchronous tasks that send messages to Kafka via the producer object
    Args:
        producer: (KafkaProducer) The producer object that interfaces with Kafka
        number_of_producers: (int) The total number of producers we want available to send messages
        endless_messages: (bool) A flag that tells the producers to keep sending messages
    """
    tasks: list[asyncio.Task] = []
    for index in range(num_of_producers):
        tasks.append(
            asyncio.create_task(
                send_messages(f"Task {index}", producer, endless_messages)
            )
        )
    return tasks


def create_consumers(number_of_consumers: int) -> list[KafkaConsumer]:
    """
    Explanation:
        Create the consumers that will interface with Kafka using configurations
    Args:
        number_of_consumers: (int) The total number of consumers we want available to poll
    """
    consumers: list[KafkaConsumer] = []
    for _ in range(number_of_consumers):
        consumers.append(
            KafkaConsumer(
                bootstrap_servers="localhost:9092",
                group_id="group_test",
                security_protocol="SSL",
                ssl_check_hostname=False,
                ssl_cafile=os.path.join(os.path.dirname(__file__), "secrets/ca.pem"),
                ssl_certfile=os.path.join(
                    os.path.dirname(__file__), "secrets/client-cert.pem"
                ),
                ssl_keyfile=os.path.join(
                    os.path.dirname(__file__), "secrets/client-key.pem"
                ),
                ssl_password="password",
            )
        )
    return consumers


def assign_consumer_partitions(consumers: list[KafkaConsumer]) -> None:
    """
    Explanation:
        Assign the partitions each consumer will poll from
    Args:
        consumers: (list[KafkaConsumer]) The consumer objects that interface with Kafka
    """
    for index, consumer in enumerate(consumers):
        consumer.assign([TopicPartition("ASYNC", index)])
        print(f"Consumer {index}'s partition:", consumer.assignment())


def create_threads(consumers: list[KafkaConsumer]) -> None:
    """
    Explanation:
        Create the threads that poll for new data from Kafka
    Args:
        consumers: (list[KafkaConsumer]) The consumer objects that interface with Kafka
    """
    for consumer in consumers:
        thread1 = threading.Thread(
            target=worker, args=[consumer, asyncio.get_running_loop()]
        )
        thread1.start()


async def main():
    """
    Explanation:
        The main method is responsible for generating messages, creating consumers, preparing
        the threads to poll for messages, and scheduling the async logging functions.
    """
    try:
        # Create messages
        producer: KafkaProducer = create_producer()

        # Generate messages through asynchronous tasks
        await assign_producers(producer, NUM_OF_PRODUCERS, ENDLESS_MESSAGES)
        print("All messages sent!")

        # Create consumers
        consumers: list[KafkaConsumer] = create_consumers(NUM_OF_CONSUMERS)

        # Assign consumers
        assign_consumer_partitions(consumers)

        # Create logger
        log_object = Logger("text.txt")

        # Create log coroutines
        tasks = [
            asyncio.create_task(logger(log_object)),
            asyncio.create_task(logger(log_object)),
        ]

        # Start threads
        create_threads(consumers)

        await asyncio.gather(*tasks)

    except Exception as exc:
        raise exc


asyncio.run(main())
