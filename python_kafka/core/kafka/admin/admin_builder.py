"""
The builder class for Admin.
"""

from .admin import Admin


class AdminBuilder:
    """
    A builder class for creating and configuring instances of the `Admin` class.

    Attributes:
    -----------
    _bootstrap_servers : str
        The Kafka bootstrap servers (default: "localhost:9092").
    _security_protocol : str
        The security protocol for connecting to Kafka (default: "SSL").
    _ssl_check_hostname : bool
        Whether to check the hostname in SSL certificates (default: False).

    Methods:
    --------
    bootstrap_servers(bootstrap_servers: str) -> "AdminBuilder"
        Sets the Kafka bootstrap server(s). Raises ValueError if bootstrap_servers is not a string.

    security_protocol(security_protocol: str) -> "AdminBuilder"
        Sets the security protocol for Kafka (e.g., "SSL", "PLAINTEXT").
        Raises ValueError if security_protocol is not a string.

    ssl_check_hostname(ssl_check_hostname: bool) -> "AdminBuilder"
        Sets whether to check the hostname in SSL certificates.
        Raises ValueError if ssl_check_hostname is not a boolean.

    build() -> Admin
        Creates a new `Admin` instance with the configured parameters.
    """

    def __init__(self):
        """
        Initializes the AdminBuilder with default values for all attributes:
        - bootstrap_servers: "localhost:9092"
        - security_protocol: "SSL"
        - ssl_check_hostname: False
        """
        self._bootstrap_servers = "localhost:9092"
        self._security_protocol = "SSL"
        self._ssl_check_hostname = False

    def bootstrap_servers(self, bootstrap_servers: str) -> "AdminBuilder":
        """
        Sets the Kafka bootstrap server(s).

        Parameters:
        -----------
        bootstrap_servers : str
            The Kafka server(s) to connect to.

        Returns:
        --------
        AdminBuilder:
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

    def security_protocol(self, security_protocol: str) -> "AdminBuilder":
        """
        Sets the security protocol for connecting to Kafka.

        Parameters:
        -----------
        security_protocol : str
            The security protocol to use (e.g., "SSL", "PLAINTEXT").

        Returns:
        --------
        AdminBuilder:
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

    def ssl_check_hostname(self, ssl_check_hostname: bool) -> "AdminBuilder":
        """
        Sets whether to check the hostname in SSL certificates.

        Parameters:
        -----------
        ssl_check_hostname : bool
            Whether to check the hostname.

        Returns:
        --------
        AdminBuilder:
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

    def build(self) -> Admin:
        """
        Builds and returns an `Admin` instance with the configured parameters.

        Returns:
        --------
        Admin:
            A new instance of the `Admin` class.
        """
        return Admin(
            self._bootstrap_servers, self._security_protocol, self._ssl_check_hostname
        )
