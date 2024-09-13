import os
import asyncio
import signal
import sys
from python_kafka.core.kafka.consumer.consumer_builder import ConsumerBuilder
from python_kafka.core.kafka.consumer.consumer import Consumer

def handle_sigterm() -> None:
    """
    Handle SIGTERM
    """
    throughput: int = 0

    for n in consumers:
        # Access Kafka consumer metrics
        metrics = n.metrics()
        print(metrics)

    sys.exit(0)

consumers: list[Consumer] = []

async def main():
    signal.signal(signal.SIGTERM, handle_sigterm)

    bootstrap_servers: str = os.environ["BOOTSTRAP_SERVERS"]
    security_protocol: str = os.environ["SECURITY_PROTOCOL"]
    ssl_check_hostname: bool = os.environ["SSL_CHECK_HOSTNAME"]
    consumers: int = os.environ["CONSUMERS"]
    group: str = os.environ["GROUP"]
    topic: str = os.environ["TOPIC"]
    timeout: int = 100
    max_records = 100

    tasks: list[asyncio.Task] = []

    for n in range(consumers):
        consumer: Consumer = (
            ConsumerBuilder()
                .bootstrap_servers(bootstrap_servers)
                .security_protocol(security_protocol)
                .ssl_check_hostname(ssl_check_hostname)
                .group(group)
                .topic(topic)
                .build()
        )
        tasks.append(asyncio.create_task(consumer.consume_messages(timeout=timeout, max_records=max_records)))
        consumers.append(consumer)

    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
