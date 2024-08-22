import asyncio
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    security_protocol="SSL",
    ssl_cafile="./secrets/ca.pem",
    ssl_certfile="./secrets/client-cert.pem",
    ssl_keyfile="./secrets/client_key.pem",
    ssl_password="password"
)

async def send_messages(task_name: str) -> None:
    """
    Explanation:
        Send 100 messages
    """
    for i in range(100):
        producer.send(topic="ASYNC", value=f"Message {i} from {task_name}".encode())
    producer.flush()
