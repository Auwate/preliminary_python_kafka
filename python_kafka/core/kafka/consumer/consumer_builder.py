"""
The builder class for creating and configuring instances of the `Consumer` class.

This class provides a fluent interface for setting up a `Consumer` instance with various
configuration options, including Kafka topic, group ID, bootstrap servers, security protocol,
and SSL settings.

Attributes:
-----------
_topic : str
    The Kafka topic from which messages will be consumed (default: "").
_group : str
    The Kafka consumer group ID (default: "").
_bootstrap_servers : str
    The Kafka bootstrap servers (default: "localhost:9092").
_security_protocol : str
    The security protocol for connecting to Kafka (default: "SSL").
_ssl_check_hostname : bool
    Whether to check the hostname in SSL certificates (default: False).
_queue : asyncio.Queue
    An asyncio queue where consumed messages will be placed (default: asyncio.Queue()).

Methods:
--------
topic(topic: str) -> "ConsumerBuilder"
    Sets the Kafka topic to consume messages from. Raises ValueError if the topic is not a string.

group(group: str) -> "ConsumerBuilder"
    Sets the Kafka consumer group ID. Raises ValueError if the group is not a string.

bootstrap_servers(bootstrap_servers: str) -> "ConsumerBuilder"
    Sets the Kafka bootstrap server(s). Raises ValueError if bootstrap_servers is not a string.

security_protocol(security_protocol: str) -> "ConsumerBuilder"
    Sets the security protocol for Kafka (e.g., "SSL", "PLAINTEXT"). Raises ValueError
    if security_protocol is not a string.

ssl_check_hostname(ssl_check_hostname: bool) -> "ConsumerBuilder"
    Sets whether to check the hostname in SSL certificates. Raises ValueError if
    ssl_check_hostname is not a boolean.

queue(queue: asyncio.Queue) -> "ConsumerBuilder"
    Sets the asyncio queue where consumed messages will be placed. Raises ValueError if
    queue is not an instance of asyncio.Queue.

build() -> Consumer
    Creates a new `Consumer` instance with the configured parameters.
"""

import asyncio
from .consumer import Consumer


class ConsumerBuilder:
    """
    A builder class for creating and configuring instances of the `Consumer` class.

    This class provides a fluent interface for setting up a `Consumer` instance with various
    configuration options, including Kafka topic, group ID, bootstrap servers, security protocol,
    and SSL settings.

    Attributes:
    -----------
    _topic : str
        The Kafka topic from which messages will be consumed (default: "").
    _group : str
        The Kafka consumer group ID (default: "").
    _bootstrap_servers : str
        The Kafka bootstrap servers (default: "localhost:9092").
    _security_protocol : str
        The security protocol for connecting to Kafka (default: "SSL").
    _ssl_check_hostname : bool
        Whether to check the hostname in SSL certificates (default: False).
    _queue : asyncio.Queue
        An asyncio queue where consumed messages will be placed (default: asyncio.Queue()).

    Methods:
    --------
    topic(topic: str) -> "ConsumerBuilder"
        Sets the Kafka topic to consume messages from. Raises ValueError if the topic is not a
        string.

    group(group: str) -> "ConsumerBuilder"
        Sets the Kafka consumer group ID. Raises ValueError if the group is not a string.

    bootstrap_servers(bootstrap_servers: str) -> "ConsumerBuilder"
        Sets the Kafka bootstrap server(s). Raises ValueError if bootstrap_servers is not a string.

    security_protocol(security_protocol: str) -> "ConsumerBuilder"
        Sets the security protocol for Kafka (e.g., "SSL", "PLAINTEXT"). Raises ValueError if
        security_protocol is not a string.

    ssl_check_hostname(ssl_check_hostname: bool) -> "ConsumerBuilder"
        Sets whether to check the hostname in SSL certificates. Raises ValueError if
        ssl_check_hostname is not a boolean.

    queue(queue: asyncio.Queue) -> "ConsumerBuilder"
        Sets the asyncio queue where consumed messages will be placed. Raises ValueError if
        queue is not an instance of asyncio.Queue.

    build() -> Consumer
        Creates a new `Consumer` instance with the configured parameters.
    """

    def __init__(self):
        """
        Initializes the ConsumerBuilder with default values for all attributes:
        - topic: an empty string
        - group: an empty string
        - bootstrap_servers: "localhost:9092"
        - security_protocol: "SSL"
        - ssl_check_hostname: False
        - queue: an empty asyncio.Queue
        """
        self._topic = ""
        self._group = ""
        self._bootstrap_servers = "localhost:9092"
        self._security_protocol = "SSL"
        self._ssl_check_hostname = False
        self._queue = asyncio.Queue()

    def topic(self, topic: str) -> "ConsumerBuilder":
        """
        Sets the Kafka topic to consume messages from.

        Parameters:
        -----------
        topic : str
            The topic to set.

        Returns:
        --------
        ConsumerBuilder:
            The current instance of the factory for method chaining.

        Raises:
        -------
        ValueError:
            If the topic is not of type str.
        """
        if not topic:
            return self
        if not isinstance(topic, str):
            raise ValueError("Topic is not of type str.")
        self._topic = topic
        return self

    def group(self, group: str) -> "ConsumerBuilder":
        """
        Sets the Kafka consumer group ID.

        Parameters:
        -----------
        group : str
            The group ID to set.

        Returns:
        --------
        ConsumerBuilder:
            The current instance of the factory for method chaining.

        Raises:
        -------
        ValueError:
            If the group is not of type str.
        """
        if not group:
            return self
        if not isinstance(group, str):
            raise ValueError("Group is not of type str.")
        self._group = group
        return self

    def queue(self, queue: asyncio.Queue) -> "ConsumerBuilder":
        """
        Sets the asyncio queue where consumed messages will be placed.

        Parameters:
        -----------
        queue : asyncio.Queue
            The queue to use for placing consumed messages.

        Returns:
        --------
        ConsumerBuilder:
            The current instance of the factory for method chaining.

        Raises:
        -------
        ValueError:
            If queue is not an instance of asyncio.Queue.
        """
        if not queue:
            return self
        if not isinstance(queue, asyncio.Queue):
            raise ValueError("Queue is not of type asyncio.Queue")
        self._queue = queue
        return self

    def build(self) -> "Consumer":
        """
        Builds and returns a `Consumer` instance with the configured parameters.

        Returns:
        --------
        Consumer:
            A new instance of the `Consumer` class.
        """
        return Consumer(
            bs_servers=self._bootstrap_servers,
            sec_protocol=self._security_protocol,
            ssl_check_hostname=self._ssl_check_hostname,
            topic=self._topic,
            group=self._group,
            queue=self._queue,
        )
