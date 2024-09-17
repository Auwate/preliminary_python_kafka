
import asyncio
import datetime
import random
from kafka.consumer.fetcher import ConsumerRecord
from kafka import KafkaConsumer, TopicPartition
from ....configs.configs import ssl_cafile, ssl_certfile, ssl_keyfile, ssl_password


class Consumer:
    def __init__(
        self,
        bs_servers: str,
        sec_protocol: str,
        ssl_check_hostname: bool,
        topic: str,
        group: str,
    ) -> None:
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
        return self._shutdown

    @shutdown.setter
    def shutdown(self, shutdown: bool) -> None:
        if not isinstance(shutdown, bool):
            raise ValueError("Shutdown flag must be a boolean value.")
        self._shutdown = shutdown

    @property
    def consumer(self) -> KafkaConsumer:
        return self._consumer

    async def consume_messages(self, timeout: int, max_records: int) -> int:
        consumed = 0
        while not self.shutdown:
            try:
                data: dict[TopicPartition, list[ConsumerRecord]] = self.consumer.poll(timeout_ms=timeout, max_records=max_records)
                if data:
                    # Simulate processing delay for each batch of messages
                    consumed += len(data)
                await asyncio.sleep(len(data) * 0.005 * random.randint(1, 10))
            except Exception as exc:  # pylint: disable=W0718
                print(f"\nERROR: {datetime.datetime.now()}: {exc}\n")
                await asyncio.sleep(1 * 0.005 * random.randint(1, 10))

        return consumed
