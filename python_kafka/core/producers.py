"""
The producers module that creates messages for Kafka
"""

from kafka import KafkaProducer, errors as Errors
from kafka.producer.future import FutureRecordMetadata


def _test_connection(
    producer: KafkaProducer, topic: str
) -> bool | Errors.KafkaTimeoutError | Exception:
    """
    Explanation:
        Sends a single message to Kafka to test the connection.
    Args:
        producer: (KafkaProducer) A producer object that interfaces with the Kafka node
        topic: (str) The topic we would like to test our connection with
    """
    try:
        future: FutureRecordMetadata = producer.send(topic=topic, value="Test".encode())
        metadata = future.get()
        if metadata.topic != topic:
            return False
    except Errors.KafkaTimeoutError as exc:
        raise exc
    except Exception as exc:
        raise exc

    return True


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
            try:
                producer.send(
                    topic="ASYNC", value=f"Message {i} from {task_name}".encode()
                )
            except Errors.KafkaTimeoutError as exc:
                raise exc
        producer.flush()
        if not endless_messages:
            break
