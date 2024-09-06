"""
The consumers module for receiving messages from Kafka and logging it
"""

import asyncio
from kafka import KafkaConsumer
from .logger import Logger

queue = asyncio.Queue()


def worker(
    consumer: KafkaConsumer,
    loop: asyncio.AbstractEventLoop,
    timeout: int = 20,
    max_records: int = 100,
) -> None:
    """
    Explanation:
        A function that worker threads use to poll for new data from Kafka.
    Args:
        consumer: (KafkaConsumer) The consumer object that interfaces with Kafka
        loop: (AbstractEventLoop) The event loop that allows the thread to push data into the
            queue asynchronously
        timeout: (int) The amount of time (in milliseconds) that the consumer will wait in
            the poll() function
        max_records: (int) The maximum amount of records the thread can poll for
    """
    while True:
        data = consumer.poll(timeout_ms=timeout, max_records=max_records)
        if data:
            asyncio.run_coroutine_threadsafe(queue.put(data), loop=loop)


async def logger(log_obj: Logger):
    """
    Explanation:
        An asynchronous function that consumes data from the Queue and sends it to the logger.
    Args:
        log_obj: (Logger) A singleton logging object that currently prints to a file
            called text.txt
    """
    print("Created logger")
    try:
        while True:
            print("waiting")
            data = await asyncio.wait_for(queue.get(), timeout=10)
            print(data)
            data = list(data.values())[0]
            log_obj.log(data)
    except Exception as exc:
        raise ValueError from exc
