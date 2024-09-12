"""
A wrapper class for the KafkaProducer class. Works in tandem with the producerBuilder.py file
"""

from kafka import KafkaProducer, errors
from ....configs.configs import ssl_cafile, ssl_certfile, ssl_keyfile, ssl_password


class Producer:
    """
    A wrapper class for KafkaProducer that handles message production to a Kafka topic.

    Attributes:
    -----------
    _volume : int
        The number of messages to send.
    _topic : str
        The Kafka topic to send messages to.
    _producer : KafkaProducer
        An instance of KafkaProducer used to send messages to the Kafka broker.

    Parameters:
    -----------
    volume : int
        The number of messages to send in the stream.
    topic : str
        The topic where the messages will be produced.
    bs_servers : str
        The bootstrap server(s) for Kafka, e.g., "localhost:9092".
    sec_protocol : str
        The security protocol to use (e.g., "SSL").
    check_hostname : bool
        Whether to check the hostname in SSL certificates.

    Methods:
    --------
    send_messages() -> tuple[bool, Exception]
        Sends a stream of messages to the Kafka topic.
        Returns a tuple indicating success or failure and an exception, if any.
    """

    def __init__(   # pylint: disable=R0913
        self,
        volume: int,
        topic: str,
        bs_servers: str,
        sec_protocol: str,
        check_hostname: bool,
    ):
        """
        Initializes the Producer with the specified configurations.

        Parameters:
        -----------
        volume : int
            The number of messages to send.
        topic : str
            The topic to send the messages to.
        bs_servers : str
            The bootstrap server(s) for the Kafka cluster.
        sec_protocol : str
            The security protocol to use (e.g., "SSL").
        check_hostname : bool
            Whether to check the hostname in the SSL certificate.
        """
        self._volume = volume
        self._topic = topic
        self._producer = KafkaProducer(
            bootstrap_servers=bs_servers,
            security_protocol=sec_protocol,
            ssl_check_hostname=check_hostname,
            ssl_cafile=ssl_cafile,
            ssl_certfile=ssl_certfile,
            ssl_keyfile=ssl_keyfile,
            ssl_password=ssl_password,
        )

    @property
    def volume(self) -> int:
        """Returns the number of messages to send."""
        return self._volume

    @property
    def topic(self) -> str:
        """Returns the Kafka topic where messages will be sent."""
        return self._topic

    @property
    def producer(self) -> KafkaProducer:
        """Returns the KafkaProducer instance."""
        return self._producer

    def send_messages(self) -> tuple[bool, Exception]:
        """
        Sends a stream of messages to the Kafka topic.

        For each message, the value is encoded in bytes. If an error occurs during
        message production (e.g., KafkaTimeoutError), the function returns False
        and the exception. Otherwise, it returns True and None.

        Returns:
        --------
        tuple[bool, Exception]:
            A tuple where the first value is True if all messages were successfully sent,
            or False if an exception occurred. The second value is the exception (if any),
            or None if successful.
        """
        for i in range(self.volume):
            try:
                self.producer.send(topic=self.topic, value=f"Message {i}\n".encode())
            except errors.KafkaTimeoutError as exc:
                return False, exc
        return True, None
