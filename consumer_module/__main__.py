import os
import asyncio
import signal
import datetime
from kafka import TopicPartition
from python_kafka.core.kafka.consumer.consumer_builder import ConsumerBuilder
from python_kafka.core.kafka.consumer.consumer import Consumer

async def handle_sigterm(sig: signal.Signals, loop: asyncio.AbstractEventLoop) -> None:
    """
    Handle SIGTERM
    """
    print(f"\nINFO: {datetime.datetime.now()}: SIGNAL {sig} received...\n", flush=True)

    for n in consumer_list:
        n.shutdown = True

    await asyncio.gather(*tasks)

    amount_consumed = 0

    try:
        for task in tasks:
            amount_consumed += task.result()

    except Exception as exc:
        raise exc

    print(f"\nINFO: {datetime.datetime.now()}: Amount consumed - {amount_consumed}\n", flush=True)

    loop.close()


consumer_list: list[Consumer] = []
tasks: list[asyncio.Task] = []

async def main():

    loop = asyncio.get_event_loop()

    loop.add_signal_handler(
        signal.SIGTERM,
        lambda s=signal.SIGTERM: asyncio.create_task(handle_sigterm(s, loop))
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

    for i in range(consumers):
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
            tasks.append(
                asyncio.create_task(
                    coro = consumer.consume_messages(timeout=timeout, max_records=max_records)
                )
            )
            consumer_list.append(consumer)
        except Exception as exc:
            raise exc
        print("Ready!")
        await asyncio.gather(*tasks)
        print(f"\nWARN: {datetime.datetime.now()}: A possible error may have occurred in consume_messages\n", flush=True)

if __name__ == "__main__":
    asyncio.run(main())
