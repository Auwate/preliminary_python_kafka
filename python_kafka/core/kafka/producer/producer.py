"""
A wrapper class for the KafkaProducer class. Works in tandem with the producerBuilder.py file.
"""

import datetime
import asyncio
from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaProducer, errors
from kafka.producer.future import FutureRecordMetadata, RecordMetadata
from ....configs.configs import ssl_cafile, ssl_certfile, ssl_keyfile, ssl_password


class Producer:
    """
    A wrapper class around KafkaProducer for sending messages to Kafka topics.

    Attributes:
        _shutdown (bool): Flag indicating whether the producer is in shutdown state.
        _topic (str): The Kafka topic where messages will be sent.
        _producer (KafkaProducer): The KafkaProducer instance used to send messages.
    """

    def __init__(  # pylint: disable=R0913
        self,
        topic: str,
        acks: str,
        bs_servers: str,
        sec_protocol: str,
        check_hostname: bool,
    ):
        """
        Initializes the Producer instance with the provided settings.

        Args:
            topic (str): The Kafka topic where messages will be sent.
            acks (str): Acknowledgment level (0, 1, or "all").
            bs_servers (str): Bootstrap servers for Kafka connection.
            sec_protocol (str): Security protocol for Kafka connection.
            check_hostname (bool): Whether to check SSL hostname.
        """
        self._shutdown = False
        self._topic = topic
        self._producer = KafkaProducer(
            bootstrap_servers=bs_servers,
            security_protocol=sec_protocol,
            ssl_check_hostname=check_hostname,
            acks=acks,
            ssl_cafile=ssl_cafile,
            ssl_certfile=ssl_certfile,
            ssl_keyfile=ssl_keyfile,
            ssl_password=ssl_password,
        )

    @property
    def topic(self) -> str:
        """
        Returns the Kafka topic where messages will be sent.

        Returns:
            str: The Kafka topic.
        """
        return self._topic

    @property
    def producer(self) -> KafkaProducer:
        """
        Returns the KafkaProducer instance.

        Returns:
            KafkaProducer: The KafkaProducer instance.
        """
        return self._producer

    @property
    def shutdown(self) -> bool:
        """
        Returns the shutdown state of the producer.

        Returns:
            bool: The shutdown state.
        """
        return self._shutdown

    @shutdown.setter
    def shutdown(self, shutdown) -> None:
        """
        Sets the shutdown state of the producer.

        Args:
            shutdown (bool): The shutdown state.

        Raises:
            ValueError: If shutdown is not of type bool.
        """
        if not isinstance(shutdown, bool):
            raise ValueError("Shutdown is not of type bool.")
        self._shutdown = shutdown

    async def send_messages(self, executor: ThreadPoolExecutor) -> int:
        """
        Sends messages to the Kafka topic asynchronously.

        Args:
            executor (ThreadPoolExecutor): The executor to run synchronous operations in.

        Returns:
            int: The number of messages successfully sent.
        """
        message_count = 0
        loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
        while not self.shutdown:
            try:
                future: FutureRecordMetadata = self.producer.send(
                    topic=self.topic, value=f"Message {message_count+1}\n".encode()
                )
                result: RecordMetadata = await loop.run_in_executor(
                    executor=executor, func=lambda: future.get(timeout=10)
                )

                if result.topic == self.topic:
                    message_count += 1
            except errors.KafkaTimeoutError as exc:
                print(f"\nERROR: {datetime.datetime.now()}: {exc}\n")

        self.producer.flush()
        return message_count
