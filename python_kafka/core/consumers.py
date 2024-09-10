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
    missed_polls = 0

    while True:
        if missed_polls == 15:
            return
        data = consumer.poll(timeout_ms=timeout, max_records=max_records)
        missed_polls += 1
        if data:
            missed_polls -= 1
            try:
                asyncio.run_coroutine_threadsafe(queue.put(data), loop=loop)
            except Exception as exc:
                raise exc


async def logger(log_obj: Logger):
    """
    Explanation:
        An asynchronous function that consumes data from the Queue and sends it to the logger.
    Args:
        log_obj: (Logger) A singleton logging object that currently prints to a file
            called text.txt
    """
    print("Created logger")
    while True:
        print("waiting")
        try:
            data = await asyncio.wait_for(queue.get(), timeout=10)
        except TimeoutError as exc:
            raise TimeoutError("Timeout has been reached for logger") from exc
        except Exception as exc:
            raise exc
        if data:
            print(data)
            data = list(data.values())[0]
            log_obj.log(data)
