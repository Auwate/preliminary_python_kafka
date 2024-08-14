import asyncio
from kafka import KafkaConsumer
from .logger import Logger

queue = asyncio.Queue()

def worker(consumer: KafkaConsumer, loop, timeout: int = 20, max_records: int = 100) -> None:
    while True:
        data = consumer.poll(
            timeout_ms=timeout,
            max_records=max_records
        )
        if data:
            asyncio.run_coroutine_threadsafe(queue.put(data), loop=loop)

async def logger(log_obj: Logger):
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
