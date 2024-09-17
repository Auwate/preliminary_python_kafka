import os
import asyncio
import signal
import datetime
from types import FrameType
from kafka import TopicPartition
from python_kafka.core.kafka.consumer.consumer_builder import ConsumerBuilder
from python_kafka.core.kafka.consumer.consumer import Consumer

def handle_sigterm(sig: signal.Signals) -> None:
    """
    Handle SIGTERM
    """
    print(f"\nINFO: {datetime.datetime.now()}: SIGNAL {sig} received...\n", flush=True)

    for n in consumer_list:
        n.shutdown = True


consumer_list: list[Consumer] = []
tasks: list[asyncio.Task] = []

async def main():

    loop = asyncio.get_event_loop()

    loop.add_signal_handler(
        signal.SIGTERM,
        lambda s=signal.SIGTERM: asyncio.create_task(handle_sigterm(s))
    )

    loop.add_signal_handler(
        signal.SIGTERM,
        lambda s=signal.SIGINT: asyncio.create_task(handle_sigterm(s))
    )

    bootstrap_servers: str = os.environ["BOOTSTRAP_SERVERS"]
    security_protocol: str = os.environ["SECURITY_PROTOCOL"]
    ssl_check_hostname: bool = os.environ["SSL_CHECK_HOSTNAME"]

    if ssl_check_hostname == "False":
        ssl_check_hostname = False
    else:
        ssl_check_hostname = True

    consumers: int = int(os.environ["CONSUMERS"])
    group: str = os.environ["GROUP"]
    topic: str = os.environ["TOPIC"]
    timeout: int = 100
    max_records = 100

    print(f"\nINFO: {datetime.datetime.now()}: TEST MESSAGE\n", flush=True)

    print("BS:", bootstrap_servers, "SP:", security_protocol, "SCH", ssl_check_hostname, "Group", group, "Topic", topic, "Consumers:", consumers, flush=True)

    for i in range(consumers):
        print("INSIDE LOOP NUMBER", i, flush=True)
        try:
            consumer: Consumer = (
                ConsumerBuilder()
                    .bootstrap_servers(bootstrap_servers)
                    .security_protocol(security_protocol)
                    .ssl_check_hostname(ssl_check_hostname)
                    .group(group)
                    .topic(None)
                    .build()
            )
            consumer.consumer.assign([TopicPartition(topic, i)])
            print("Consumer:", consumer, flush=True)
            tasks.append(
                asyncio.create_task(
                    coro = consumer.consume_messages(timeout=timeout, max_records=max_records)
                )
            )
            consumer_list.append(consumer)
            print("CL:", consumer_list, "Tasks:", tasks, flush=True)
        except Exception as exc:
            raise exc

    print("Waiting for gather...", flush=True)
    await asyncio.gather(*tasks)

    amount_consumed = 0

    try:
        for task in tasks:
            amount_consumed += task.result()

    except Exception as exc:
        raise exc

    print(f"\nINFO: {datetime.datetime.now()}: Amount consumed - {amount_consumed}\n", flush=True)

if __name__ == "__main__":
    asyncio.run(main())
