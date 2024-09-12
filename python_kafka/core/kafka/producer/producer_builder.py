"""
The builder class for Producer
"""

from .producer import Producer


class ProducerBuilder:
    """
    A builder class for creating and configuring instances of the `Producer` class.

    Attributes:
    -----------
    _topic : str
        The Kafka topic where messages will be sent (default: "").
    _volume : int
        The number of messages to send (default: 0).
    _bootstrap_servers : str
        The Kafka bootstrap servers (default: "localhost:9092").
    _security_protocol : str
        The security protocol for connecting to Kafka (default: "SSL").
    _ssl_check_hostname : bool
        Whether to check the hostname in SSL certificates (default: False).

    Methods:
    --------
    topic(topic: str) -> "ProducerFactory"
        Sets the Kafka topic to send messages to. Raises ValueError if the topic is not a string.

    volume(volume: int) -> "ProducerFactory"
        Sets the number of messages to send. Raises ValueError if volume is not an integer.

    bootstrap_servers(bootstrap_servers: str) -> "ProducerFactory"
        Sets the Kafka bootstrap server(s). Raises ValueError if bootstrap_servers is not a string.

    security_protocol(security_protocol: str) -> "ProducerFactory"
        Sets the security protocol for Kafka (e.g., "SSL", "PLAINTEXT").
        Raises ValueError if security_protocol is not a string.

    ssl_check_hostname(ssl_check_hostname: bool) -> "ProducerFactory"
        Sets whether to check the hostname in SSL certificates.
        Raises ValueError if ssl_check_hostname is not a boolean.

    build() -> Producer
        Creates a new `Producer` instance with the configured parameters.
    """

    def __init__(self):
        """
        Initializes the ProducerFactory with default values for all attributes:
        - topic: an empty string
        - volume: 0
        - bootstrap_servers: "localhost:9092"
        - security_protocol: "SSL"
        - ssl_check_hostname: False
        """
        self._topic = "Test"
        self._volume = 1_000
        self._bootstrap_servers = "localhost:9092"
        self._security_protocol = "SSL"
        self._ssl_check_hostname = False

    def topic(self, topic: str) -> "ProducerBuilder":
        """
        Sets the Kafka topic to send messages to.

        Parameters:
        -----------
        topic : str
            The topic to set.

        Returns:
        --------
        ProducerFactory:
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

    def volume(self, volume: int) -> "ProducerBuilder":
        """
        Sets the number of messages to send.

        Parameters:
        -----------
        volume : int
            The number of messages to send.

        Returns:
        --------
        ProducerFactory:
            The current instance of the factory for method chaining.

        Raises:
        -------
        ValueError:
            If volume is not of type int.
        """
        if not volume:
            return self
        if not isinstance(volume, int):
            raise ValueError("Volume is not of type int.")
        self._volume = volume
        return self

    def bootstrap_servers(self, bootstrap_servers: str) -> "ProducerBuilder":
        """
        Sets the Kafka bootstrap server(s).

        Parameters:
        -----------
        bootstrap_servers : str
            The Kafka server(s) to connect to.

        Returns:
        --------
        ProducerFactory:
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

    def security_protocol(self, security_protocol: str) -> "ProducerBuilder":
        """
        Sets the security protocol for connecting to Kafka.

        Parameters:
        -----------
        security_protocol : str
            The security protocol to use (e.g., "SSL", "PLAINTEXT").

        Returns:
        --------
        ProducerFactory:
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

    def ssl_check_hostname(self, ssl_check_hostname: bool) -> "ProducerBuilder":
        """
        Sets whether to check the hostname in SSL certificates.

        Parameters:
        -----------
        ssl_check_hostname : bool
            Whether to check the hostname.

        Returns:
        --------
        ProducerFactory:
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

    def build(self) -> Producer:
        """
        Builds and returns a `Producer` instance with the configured parameters.

        Returns:
        --------
        Producer:
            A new instance of the `Producer` class.
        """
        return Producer(
            volume=self._volume,
            topic=self._topic,
            bs_servers=self._bootstrap_servers,
            sec_protocol=self._security_protocol,
            check_hostname=self._ssl_check_hostname,
        )
