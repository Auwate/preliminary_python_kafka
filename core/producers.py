from kafka import KafkaProducer


async def send_messages(
    task_name: str, producer: KafkaProducer, endless_messages: bool
) -> None:
    """
    Explanation:
        Using the producer object, send 100 messages to Kafka.
    Args:
        task_name: (str) A string that is used to differentiate messages between producers
        producer: (KafkaProducer) A producer object that interfaces with the Kafka node
        endless_messages: (bool) A flag to tell the producer to keep sending messages endlessly
    """
    while endless_messages or not endless_messages:
        for i in range(100):
            producer.send(topic="ASYNC", value=f"Message {i} from {task_name}".encode())
        producer.flush()
        if not endless_messages:
            break
