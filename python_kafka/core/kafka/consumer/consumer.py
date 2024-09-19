"""
Consumer class for consuming messages from Kafka using the KafkaConsumer from the kafka-python
library.
"""

import asyncio
import datetime
import random
from kafka.consumer.fetcher import ConsumerRecord
from kafka import KafkaConsumer, TopicPartition
from ....configs.configs import ssl_cafile, ssl_certfile, ssl_keyfile, ssl_password


class Consumer:
    """
    A Kafka consumer that consumes messages from a specified Kafka topic.

    Attributes:
        _shutdown (bool): Flag indicating whether the consumer should shut down.
        _consumer (KafkaConsumer): The KafkaConsumer instance used for message consumption.
    """

    def __init__(  # pylint: disable=R0913
        self,
        bs_servers: str,
        sec_protocol: str,
        ssl_check_hostname: bool,
        topic: str,
        group: str,
    ) -> None:
        """
        Initializes the Consumer with Kafka server settings and SSL configurations.

        Args:
            bs_servers (str): Bootstrap servers for Kafka connection.
            sec_protocol (str): Security protocol for Kafka connection.
            ssl_check_hostname (bool): Whether to check SSL hostname.
            topic (str): Kafka topic to consume messages from. If empty, subscribes to all topics.
            group (str): Kafka consumer group ID.
        """
        self._shutdown = False

        if not topic:
            self._consumer = KafkaConsumer(
                bootstrap_servers=bs_servers,
                group_id=group,
                security_protocol=sec_protocol,
                ssl_check_hostname=ssl_check_hostname,
                ssl_cafile=ssl_cafile,
                ssl_certfile=ssl_certfile,
                ssl_keyfile=ssl_keyfile,
                ssl_password=ssl_password,
            )
        else:
            self._consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bs_servers,
                group_id=group,
                security_protocol=sec_protocol,
                ssl_check_hostname=ssl_check_hostname,
                ssl_cafile=ssl_cafile,
                ssl_certfile=ssl_certfile,
                ssl_keyfile=ssl_keyfile,
                ssl_password=ssl_password,
            )

    @property
    def shutdown(self) -> bool:
        """
        Gets the shutdown flag.

        Returns:
            bool: The shutdown flag.
        """
        return self._shutdown

    @shutdown.setter
    def shutdown(self, shutdown: bool) -> None:
        """
        Sets the shutdown flag.

        Args:
            shutdown (bool): The shutdown flag.

        Raises:
            ValueError: If shutdown is not of type bool.
        """
        if not isinstance(shutdown, bool):
            raise ValueError("Shutdown flag must be a boolean value.")
        self._shutdown = shutdown

    @property
    def consumer(self) -> KafkaConsumer:
        """
        Gets the KafkaConsumer instance.

        Returns:
            KafkaConsumer: The KafkaConsumer instance.
        """
        return self._consumer

    async def consume_messages(self, timeout: int, max_records: int) -> tuple[int, int]:
        """
        Consumes messages from Kafka asynchronously.

        Args:
            timeout (int): The maximum amount of time to wait for messages, in milliseconds.
            max_records (int): The maximum number of records to return in a single call.

        Returns:
            tuple:
                int: The number of consumed messages.
                int: The number of messages consumed per second (rounded down)
        """
        consumed = 0
        start = datetime.datetime.now()

        while not self.shutdown:
            try:
                data: dict[TopicPartition, list[ConsumerRecord]] = self.consumer.poll(
                    timeout_ms=timeout, max_records=max_records
                )
                if data:
                    for _, consumer_records in data.items():
                        # Simulate processing delay for each batch of messages
                        consumed += len(consumer_records)
                await asyncio.sleep(len(data) * 0.005 * random.randint(1, 10))
            except Exception as exc:  # pylint: disable=W0718
                print(f"\nERROR: {datetime.datetime.now()}: {exc}\n")
                await asyncio.sleep(1 * 0.005 * random.randint(1, 10))

        return consumed, consumed // (datetime.datetime.now() - start)
