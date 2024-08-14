import asyncio
from kafka import KafkaProducer

producer = KafkaProducer()

async def send_messages(task_name: str) -> None:
    """
    Explanation:
        Send 100 messages
    """
    for i in range(100):
        producer.send(topic="ASYNC", value=f"Message {i} from {task_name}".encode())
    producer.flush()
