import asyncio
import threading

from kafka import KafkaConsumer, TopicPartition
from core.logger import Logger
from core.consumers import worker, logger

async def main():
    try:
        # Create logger
        log_object = Logger("text.txt")

        # Start coroutines
        consumer1 = KafkaConsumer(group_id="group_test")
        consumer2 = KafkaConsumer(group_id="group_test")
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