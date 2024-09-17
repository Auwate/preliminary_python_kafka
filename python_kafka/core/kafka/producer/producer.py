"""
A wrapper class for the KafkaProducer class. Works in tandem with the producerBuilder.py file
"""

import datetime
import asyncio
from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaProducer, errors
from kafka.producer.future import FutureRecordMetadata, RecordMetadata
from ....configs.configs import ssl_cafile, ssl_certfile, ssl_keyfile, ssl_password


class Producer:

    def __init__(  # pylint: disable=R0913
        self,
        topic: str,
        acks: int | str,
        bs_servers: str,
        sec_protocol: str,
        check_hostname: bool,
    ):
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
        """Returns the Kafka topic where messages will be sent."""
        return self._topic

    @property
    def producer(self) -> KafkaProducer:
        """Returns the KafkaProducer instance."""
        return self._producer

    @property
    def shutdown(self) -> bool:
        return self._shutdown

    @shutdown.setter
    def shutdown(self, shutdown) -> None:
        if not isinstance(shutdown, bool):
            raise ValueError("Shutdown is not of type bool.")
        self._shutdown = shutdown

    async def send_messages(self, executor: ThreadPoolExecutor) -> int:
        message_count = 0
        loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
        while not self.shutdown:
            try:
                future: FutureRecordMetadata = self.producer.send(
                    topic=self.topic, value=f"Message {message_count+1}\n".encode()
                )
                result: RecordMetadata = await loop.run_in_executor(
                    executor=executor,
                    func=lambda: future.get(timeout=10)
                )

                if result.topic == self.topic:
                    message_count += 1
            except errors.KafkaTimeoutError as exc:
                print(f"\nERROR: {datetime.datetime.now()}: {exc}\n")


        self.producer.flush()
        return message_count
