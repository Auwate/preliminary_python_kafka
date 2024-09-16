import os
import asyncio
import signal
import datetime
from python_kafka.core.kafka.consumer.consumer_builder import ConsumerBuilder
from python_kafka.core.kafka.consumer.consumer import Consumer

def handle_sigterm() -> None:
    """
    Handle SIGTERM
    """
    print("Sigterm")

    for n in consumer_list:
        n.shutdown = True


consumer_list: list[Consumer] = []
tasks: list[asyncio.Task] = []

async def main():
    loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGTERM, handle_sigterm)

    bootstrap_servers: str = os.environ["BOOTSTRAP_SERVERS"]
    security_protocol: str = os.environ["SECURITY_PROTOCOL"]
    ssl_check_hostname: bool = bool(os.environ["SSL_CHECK_HOSTNAME"])
    consumers: int = int(os.environ["CONSUMERS"])
    group: str = os.environ["GROUP"]
    topic: str = os.environ["TOPIC"]
    timeout: int = 100
    max_records = 100

    print(consumers)

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
                coro = consumer.consume_messages(timeout=timeout, max_records=max_records)
            )
        )
        consumer_list.append(consumer)
    print("Waiting for gather...")
    await asyncio.gather(*tasks)

    amount_consumed = 0

    try:
        for task in tasks:
            amount_consumed += task.result()

    except Exception as exc:
        raise exc

    print(f"\nINFO: {datetime.datetime.now()}: Amount consumed - {amount_consumed}\n")

if __name__ == "__main__":
    asyncio.run(main())
