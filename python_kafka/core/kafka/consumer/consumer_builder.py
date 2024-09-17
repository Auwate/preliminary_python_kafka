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
        """
        self._topic = ""
        self._group = ""
        self._bootstrap_servers = "localhost:9092"
        self._security_protocol = "SSL"
        self._ssl_check_hostname = False

    def bootstrap_servers(self, bootstrap_servers: str) -> "ConsumerBuilder":
        """
        Sets the Kafka bootstrap server(s).

        Parameters:
        -----------
        bootstrap_servers : str
            The Kafka server(s) to connect to.

        Returns:
        --------
        ConsumerBuilder:
            The current instance of the factory for method chaining.

        Raises:
        -------
        ValueError:
            If bootstrap_servers is not of type str.
        """
        if not bootstrap_servers:
            return self
        if not isinstance(bootstrap_servers, str):
            raise ValueError("Bootstrap servers is not of type str.")
        self._bootstrap_servers = bootstrap_servers
        return self

    def security_protocol(self, security_protocol: str) -> "ConsumerBuilder":
        """
        Sets the security protocol for connecting to Kafka.

        Parameters:
        -----------
        security_protocol : str
            The security protocol to use (e.g., "SSL", "PLAINTEXT").

        Returns:
        --------
        ConsumerBuilder:
            The current instance of the factory for method chaining.

        Raises:
        -------
        ValueError:
            If security_protocol is not of type str.
        """
        if not security_protocol:
            return self
        if not isinstance(security_protocol, str):
            raise ValueError("Security protocol is not of type str.")
        self._security_protocol = security_protocol
        return self

    def ssl_check_hostname(self, ssl_check_hostname: bool) -> "ConsumerBuilder":
        """
        Sets whether to check the hostname in SSL certificates.

        Parameters:
        -----------
        ssl_check_hostname : bool
            Whether to check the hostname.

        Returns:
        --------
        ConsumerBuilder:
            The current instance of the factory for method chaining.

        Raises:
        -------
        ValueError:
            If ssl_check_hostname is not of type bool.
        """
        if not ssl_check_hostname:
            return self
        if not isinstance(ssl_check_hostname, bool):
            raise ValueError("SSL check hostname is not of type bool.")
        self._ssl_check_hostname = ssl_check_hostname
        return self

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
            topic=[self._topic],
            group=self._group
        )
