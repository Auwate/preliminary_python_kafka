import asyncio
import threading

from kafka import KafkaConsumer, TopicPartition
from core.logger import Logger
from core.consumers import worker, logger
from core.producers import send_messages

async def create_messages() -> None:
    tasks: list[asyncio.Task] = [
            asyncio.create_task(send_messages("Task 1")),
            asyncio.create_task(send_messages("Task 2")),
            asyncio.create_task(send_messages("Task 3"))
        ]
    await asyncio.gather(*tasks)

async def main():
    try:
        # Create messages
        await create_messages()
        print("All messages sent!")

        # Create logger
        log_object = Logger("text.txt")

        # Start coroutines
        consumer1 = KafkaConsumer(
            bootstrap_servers="localhost:9092",
            group_id="group_test",
            security_protocol="SSL",
            ssl_cafile="./secrets/ca.pem",
            ssl_certfile="./secrets/client-cert.pem",
            ssl_keyfile="./secrets/client_key.pem",
            ssl_password="password"
        )
        consumer2 = KafkaConsumer(
            bootstrap_servers="localhost:9092",
            group_id="group_test",
            security_protocol="SSL",
            ssl_cafile="./secrets/ca.pem",
            ssl_certfile="./secrets/client-cert.pem",
            ssl_keyfile="./secrets/client_key.pem",
            ssl_password="password"
        )
        consumer1.assign([TopicPartition("ASYNC", 0)])
        consumer2.assign([TopicPartition("ASYNC", 1)])
        print(consumer1.assignment())
        print(consumer2.assignment())
        tasks = [
            asyncio.create_task(logger(log_object)),
            asyncio.create_task(logger(log_object))
        ]

        # Start threads
        thread1 = threading.Thread(target=worker, args=[consumer1, asyncio.get_running_loop()])
        thread2 = threading.Thread(target=worker, args=[consumer2, asyncio.get_running_loop()])
        thread1.start()
        thread2.start()

        await asyncio.gather(*tasks)

    except Exception as exc:
        raise exc
asyncio.run(main())