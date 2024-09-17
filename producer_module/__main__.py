import os
import asyncio
import signal
import datetime
import time
from python_kafka.core.kafka.producer.producer import Producer
from python_kafka.core.kafka.producer.producer_builder import ProducerBuilder


async def handle_sigterm(sig: signal.Signals) -> None:
    """
    Handle SIGTERM
    """
    print(f"\nINFO: {datetime.datetime.now()}: SIGNAL {sig} received...\n", flush=True)

    time.sleep(4)

    for n in producer_list:
        n.shutdown = True

    await asyncio.gather(*tasks)

    amount_sent = 0

    try:
        for task in tasks:
            amount_sent += task.result()

    except Exception as exc:
        raise exc

    print(
        f"\nINFO: {datetime.datetime.now()}: Amount sent - {amount_sent}\n",
        flush=True,
    )

    time.sleep(4)

    asyncio.get_event_loop().stop()


producer_list: list[Producer] = []
tasks: list[asyncio.Task] = []


async def main():

    loop = asyncio.get_event_loop()

    loop.add_signal_handler(
        signal.SIGTERM, lambda s=signal.SIGTERM: asyncio.create_task(handle_sigterm(s))
    )

    loop.add_signal_handler(
        signal.SIGINT, lambda s=signal.SIGINT: asyncio.create_task(handle_sigterm(s))
    )

    bootstrap_servers: str = os.environ["BOOTSTRAP_SERVERS"]
    security_protocol: str = os.environ["SECURITY_PROTOCOL"]
    ssl_check_hostname: bool = os.environ["SSL_CHECK_HOSTNAME"]

    if ssl_check_hostname == "False":
        ssl_check_hostname = False
    else:
        ssl_check_hostname = True

    producers: int = int(os.environ["PRODUCERS"])
    acks: int | str = str(os.environ["ACKS"]) if os.environ["ACKS"] == "all" else int(os.environ["ACKS"])
    topic: str = os.environ["TOPIC"]

    for _ in range(producers):
        try:
            producer: Producer = (
                ProducerBuilder()
                .bootstrap_servers(bootstrap_servers)
                .security_protocol(security_protocol)
                .ssl_check_hostname(ssl_check_hostname)
                .topic(topic)
                .acks(acks)
                .build()
            )
            tasks.append(
                asyncio.create_task(
                    coro=producer.send_messages()
                )
            )
            producer_list.append(producer)
        except Exception as exc:
            raise exc

    print("Ready!", flush=True)


if __name__ == "__main__":
    loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
    try:
        loop.create_task(coro=main())
        loop.run_forever()
    except KeyboardInterrupt:
        print("Interrupted.", flush=True)
    except Exception as exc:
        raise exc
    finally:
        loop.close()
