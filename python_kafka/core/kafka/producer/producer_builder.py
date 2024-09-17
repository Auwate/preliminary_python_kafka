"""
The builder class for constructing Producer instances.
"""

from .producer import Producer


class ProducerBuilder:
    """
    A builder class for constructing a Producer instance with configurable settings.

    Attributes:
        _topic (str): The Kafka topic for the producer.
        _acks (str): Acknowledgment level for the producer (0, 1, or "all").
        _bootstrap_servers (str): Bootstrap servers for Kafka connection.
        _security_protocol (str): Security protocol for Kafka connection.
        _ssl_check_hostname (bool): Whether to check SSL hostname.
    """

    def __init__(self):
        """
        Initializes the ProducerBuilder with default settings.
        """
        self._topic = "Test"
        self._acks: str = "0"  # Can be 0, 1, or "all"
        self._bootstrap_servers = "localhost:9092"
        self._security_protocol = "SSL"
        self._ssl_check_hostname = False

    def acks(self, ack: str) -> "ProducerBuilder":
        """
        Sets the acknowledgment level for the producer.

        Args:
            ack (str): Acknowledgment level (0, 1, or "all").

        Returns:
            ProducerBuilder: The current instance of ProducerBuilder.

        Raises:
            ValueError: If ack is not of type int or str.
        """
        if not ack:
            return self
        if not isinstance(ack, str) and not isinstance(ack, int):
            raise ValueError("Ack is not of type str or int.")
        self._acks = ack
        return self

    def topic(self, topic: str) -> "ProducerBuilder":
        """
        Sets the Kafka topic for the producer.

        Args:
            topic (str): The Kafka topic.

        Returns:
            ProducerBuilder: The current instance of ProducerBuilder.

        Raises:
            ValueError: If topic is not of type str.
        """
        if not topic:
            return self
        if not isinstance(topic, str):
            raise ValueError("Topic is not of type str.")
        self._topic = topic
        return self

    def bootstrap_servers(self, bootstrap_servers: str) -> "ProducerBuilder":
        """
        Sets the bootstrap servers for Kafka connection.

        Args:
            bootstrap_servers (str): The bootstrap servers.

        Returns:
            ProducerBuilder: The current instance of ProducerBuilder.

        Raises:
            ValueError: If bootstrap_servers is not of type str.
        """
        if not bootstrap_servers:
            return self
        if not isinstance(bootstrap_servers, str):
            raise ValueError("Bootstrap servers is not of type str.")
        self._bootstrap_servers = bootstrap_servers
        return self

    def security_protocol(self, security_protocol: str) -> "ProducerBuilder":
        """
        Sets the security protocol for Kafka connection.

        Args:
            security_protocol (str): The security protocol.

        Returns:
            ProducerBuilder: The current instance of ProducerBuilder.

        Raises:
            ValueError: If security_protocol is not of type str.
        """
        if not security_protocol:
            return self
        if not isinstance(security_protocol, str):
            raise ValueError("Security protocol is not of type str.")
        self._security_protocol = security_protocol
        return self

    def ssl_check_hostname(self, ssl_check_hostname: bool) -> "ProducerBuilder":
        """
        Sets whether to check SSL hostname.

        Args:
            ssl_check_hostname (bool): Whether to check SSL hostname.

        Returns:
            ProducerBuilder: The current instance of ProducerBuilder.

        Raises:
            ValueError: If ssl_check_hostname is not of type bool.
        """
        if not ssl_check_hostname:
            return self
        if not isinstance(ssl_check_hostname, bool):
            raise ValueError("SSL check hostname is not of type bool.")
        self._ssl_check_hostname = ssl_check_hostname
        return self

    def build(self) -> Producer:
        """
        Constructs a Producer instance with the configured settings.

        Returns:
            Producer: A Producer instance with the configured settings.
        """
        return Producer(
            topic=self._topic,
            acks=self._acks,
            bs_servers=self._bootstrap_servers,
            sec_protocol=self._security_protocol,
            check_hostname=self._ssl_check_hostname,
        )
