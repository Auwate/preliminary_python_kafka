"""
ConsumerBuilder class for constructing Consumer instances with specific configurations.
"""

from .consumer import Consumer


class ConsumerBuilder:
    """
    Builder class for creating Consumer instances with customizable configurations.

    Attributes:
        _topic (str): Kafka topic to consume messages from.
        _group (str): Kafka consumer group.
        _bootstrap_servers (str): Bootstrap servers for Kafka connection.
        _security_protocol (str): Security protocol for Kafka connection.
        _ssl_check_hostname (bool): Whether to check SSL hostname.
    """

    def __init__(self):
        """
        Initializes the ConsumerBuilder with default settings.
        """
        self._topic = ""
        self._group = ""
        self._bootstrap_servers = "localhost:9092"
        self._security_protocol = "SSL"
        self._ssl_check_hostname = False

    def bootstrap_servers(self, bootstrap_servers: str) -> "ConsumerBuilder":
        """
        Sets the bootstrap servers for the Kafka connection.

        Args:
            bootstrap_servers (str): The bootstrap servers for Kafka.

        Returns:
            ConsumerBuilder: The builder instance with updated bootstrap servers.

        Raises:
            ValueError: If bootstrap_servers is not of type str.
        """
        if not bootstrap_servers:
            return self
        if not isinstance(bootstrap_servers, str):
            raise ValueError("Bootstrap servers is not of type str.")
        self._bootstrap_servers = bootstrap_servers
        return self

    def security_protocol(self, security_protocol: str) -> "ConsumerBuilder":
        """
        Sets the security protocol for the Kafka connection.

        Args:
            security_protocol (str): The security protocol for Kafka.

        Returns:
            ConsumerBuilder: The builder instance with updated security protocol.

        Raises:
            ValueError: If security_protocol is not of type str.
        """
        if not security_protocol:
            return self
        if not isinstance(security_protocol, str):
            raise ValueError("Security protocol is not of type str.")
        self._security_protocol = security_protocol
        return self

    def ssl_check_hostname(self, ssl_check_hostname: bool) -> "ConsumerBuilder":
        """
        Sets whether to check the SSL hostname.

        Args:
            ssl_check_hostname (bool): Whether to check SSL hostname.

        Returns:
            ConsumerBuilder: The builder instance with updated SSL hostname checking.

        Raises:
            ValueError: If ssl_check_hostname is not of type bool.
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

        Args:
            topic (str): The Kafka topic.

        Returns:
            ConsumerBuilder: The builder instance with updated topic.

        Raises:
            ValueError: If topic is not of type str.
        """
        if not topic:
            return self
        if not isinstance(topic, str):
            raise ValueError("Topic is not of type str.")
        self._topic = topic
        return self

    def group(self, group: str) -> "ConsumerBuilder":
        """
        Sets the Kafka consumer group.

        Args:
            group (str): The Kafka consumer group.

        Returns:
            ConsumerBuilder: The builder instance with updated consumer group.

        Raises:
            ValueError: If group is not of type str.
        """
        if group and not isinstance(group, str):
            raise ValueError("Group is not of type str.")
        self._group = group
        return self

    def build(self) -> "Consumer":
        """
        Constructs a Consumer instance with the configured settings.

        Returns:
            Consumer: The constructed Consumer instance.
        """
        return Consumer(
            bs_servers=self._bootstrap_servers,
            sec_protocol=self._security_protocol,
            ssl_check_hostname=self._ssl_check_hostname,
            topic=self._topic,
            group=self._group,
        )
