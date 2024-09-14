import os
import asyncio
import signal
import sys
from python_kafka.core.kafka.consumer.consumer_builder import ConsumerBuilder
from python_kafka.core.kafka.consumer.consumer import Consumer

def handle_sigterm(sig, frame) -> None:
    """
    Handle SIGTERM
    """
    print("Sigterm")
    throughput: int = 0

    for n in consumer_list:
        # Access Kafka consumer metrics
        metrics = n.metrics()
        print(metrics)

    sys.exit(0)

consumer_list: list[Consumer] = []

async def main():
    print("In main")
    loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGTERM, handle_sigterm)

    bootstrap_servers: str = os.environ["BOOTSTRAP_SERVERS"]
    security_protocol: str = os.environ["SECURITY_PROTOCOL"]
    ssl_check_hostname: bool = os.environ["SSL_CHECK_HOSTNAME"]
    consumers: int = os.environ["CONSUMERS"]
    group: str = os.environ["GROUP"]
    topic: str = os.environ["TOPIC"]
    timeout: int = 100
    max_records = 100

    tasks: list[asyncio.Task] = []

    for _ in range(consumers):
        consumer: Consumer = (
            ConsumerBuilder()
                .bootstrap_servers(bootstrap_servers)
                .security_protocol(security_protocol)
                .ssl_check_hostname(ssl_check_hostname)
                .group(group)
                .topic(topic)
                .build()
        )
        tasks.append(
            asyncio.create_task(
                consumer.consume_messages(timeout=timeout, max_records=max_records)
            )
        )
        consumer_list.append(consumer)

    await asyncio.gather(*tasks)

if __name__ == "__main__":
    print("here")
    asyncio.run(main())
