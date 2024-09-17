"""
A command-line interface for configuring Kafka producers and consumers.

This module provides functionality to create a command-line argument parser and
to process the arguments for setting up the number of Kafka producers and consumers.

The `CLIOptions` class allows you to specify the number of producers, consumers, 
bootstrap servers, and other properties through command-line arguments and ensures that 
these values are properly validated and converted.

Functions:
-----------
create_parser() -> argparse.ArgumentParser
    Creates and returns an argument parser for command-line arguments.

Classes:
---------
CLIOptions
    A class for handling command-line arguments for Kafka configuration.

    Attributes:
    -----------
    _producers : int
        The number of Kafka producers (default: 0).
    _consumers : int
        The number of Kafka consumers (default: 0).
    _bootstrap_server : str
        The Kafka cluster location (default: 'localhost:9092').
    _security_protocol : str
        The security protocol for Kafka communication (default: 'SSL').
    _ssl_check_hostname : bool
        Whether to check the hostname in SSL communication (default: False).

    Methods:
    --------
    __init__(parser: argparse.ArgumentParser = None, args: list[str] = None) -> None
        Initializes the CLIOptions instance and processes the provided arguments.

    read_args(parser: argparse.ArgumentParser, args: list[str]) -> None
        Reads and parses command-line arguments using the provided parser.

    evaluate_input(args: argparse.Namespace) -> None
        Validates and sets the producer, consumer, and other values from the parsed arguments.

    producers() -> int
        Returns the number of producers.

    consumers() -> int
        Returns the number of consumers.

    bootstrap_server() -> str
        Returns the bootstrap server.

    security_protocol() -> str
        Returns the security protocol.

    ssl_check_hostname() -> bool
        Returns the SSL check hostname flag.

    producers.setter(producers: int) -> None
        Sets the number of producers after validating the input.

    consumers.setter(consumers: int) -> None
        Sets the number of consumers after validating the input.

    bootstrap_server.setter(bootstrap_server: str) -> None
        Sets the bootstrap server after validating the input.

    security_protocol.setter(security_protocol: str) -> None
        Sets the security protocol after validating the input.

    ssl_check_hostname.setter(ssl_check_hostname: bool) -> None
        Sets the SSL check hostname flag after validating the input.
"""

import argparse


def create_parser() -> argparse.ArgumentParser:
    """
    Creates a command-line argument parser.

    Returns:
    --------
    argparse.ArgumentParser
        The argument parser with predefined arguments for producers, consumers,
        bootstrap server, security protocol, SSL hostname check, group, and topic.
    """
    parser = argparse.ArgumentParser(
        description="Configure Kafka producers, consumers, and other settings."
    )

    parser.add_argument(
        "-P",
        "--producers",
        help="Type: int | Specify the number of producers that will send messages to Kafka.",
        type=int,
        required=True
    )
    parser.add_argument(
        "-C",
        "--consumers",
        help="Type: int | Specify the number of consumers that will read messages from Kafka.",
        type=int,
        required=True
    )
    parser.add_argument(
        "-BS",
        "--bootstrap-server",
        help="Type: str (Optional) The location of the Kafka cluster (default: localhost:9092).",
        type=str,
    )
    parser.add_argument(
        "-SP",
        "--security-protocol",
        help="Type: str (Optional) The security protocol for Kafka communication (default: SSL).",
        type=str,
    )
    parser.add_argument(
        "-SCHN",
        "--ssl-check-hostname",
        help="Type: bool (Optional) A flag to enable SSL hostname verification (default: False).",
        type=bool,
    )
    parser.add_argument(
        "-G",
        "--group",
        help="Type: str (Optional) The Kafka consumer group ID (default: Test Group).",
        type=str,
    )
    parser.add_argument(
        "-T",
        "--topic",
        help="Type: str (Optional) The Kafka topic (default: Test Topic).",
        type=str,
    )
    parser.add_argument(
        "-A",
        "--acks",
        help="Type: int | str (Optional) The Kafka topic (default: Test Topic)."
             "Values can be 0, 1 or 'all'",
        type=int | str,
    )

    return parser


class CLIOptions:
    """
    A class to handle and validate command-line arguments for Kafka producers, consumers,
    and additional settings like bootstrap server, security protocol, and SSL hostname check.

    Attributes:
    -----------
    _producers : int
        The number of Kafka producers.
    _consumers : int
        The number of Kafka consumers.
    _bootstrap_server : str
        The Kafka cluster location.
    _security_protocol : str
        The security protocol for Kafka communication.
    _ssl_check_hostname : bool
        Whether to check the hostname in SSL communication.

    Methods:
    --------
    __init__(parser: argparse.ArgumentParser = None, args: list[str] = None) -> None
        Initializes the CLIOptions instance. Processes the provided parser and arguments.

    read_args(parser: argparse.ArgumentParser, args: list[str]) -> None
        Reads and parses command-line arguments using the provided parser.

    evaluate_input(args: argparse.Namespace) -> None
        Validates and assigns producer, consumer, and additional settings from the parsed arguments.

    producers() -> int
        Returns the number of producers.

    consumers() -> int
        Returns the number of consumers.

    bootstrap_server() -> str
        Returns the bootstrap server.

    security_protocol() -> str
        Returns the security protocol.

    ssl_check_hostname() -> bool
        Returns the SSL check hostname flag.

    producers.setter(producers: int) -> None
        Sets the number of producers after validating the input.

    consumers.setter(consumers: int) -> None
        Sets the number of consumers after validating the input.

    bootstrap_server.setter(bootstrap_server: str) -> None
        Sets the bootstrap server after validating the input.

    security_protocol.setter(security_protocol: str) -> None
        Sets the security protocol after validating the input.

    ssl_check_hostname.setter(ssl_check_hostname: bool) -> None
        Sets the SSL check hostname flag after validating the input.
    """

    def __init__(
        self, parser: argparse.ArgumentParser = None, args: list[str] = None
    ) -> None:
        """
        Initializes the CLIOptions with default values and processes the provided arguments.

        Parameters:
        -----------
        parser : argparse.ArgumentParser, optional
            The argument parser to use. If not provided, a default parser is created.
        args : list[str], optional
            A list of command-line arguments to parse. If not provided, defaults to `sys.argv`.
        """
        self._consumers = 5 # Default values in case nothing is changed
        self._producers = 5
        self._group = "Test_Group"
        self._topic = "Test_Topic"
        self._acks: int | str = 0 # Values can be 0, 1, or "all"
        self._bootstrap_server = "localhost:9092"
        self._security_protocol = "SSL"
        self._ssl_check_hostname = False
        self.read_args(parser, args)

    def read_args(self, parser: argparse.ArgumentParser, args: list[str]) -> None:
        """
        Reads and parses command-line arguments.

        Parameters:
        -----------
        parser : argparse.ArgumentParser
            The argument parser to use for reading arguments.
        args : list[str]
            The list of command-line arguments to parse.
        """
        if not parser:
            parser = create_parser()
        arguments = parser.parse_args(args)
        self.evaluate_input(arguments)

    def evaluate_input(self, args: argparse.Namespace) -> None:
        """
        Validates and assigns the values of producers, consumers, group, topic, and other settings
        from the parsed arguments.

        Parameters:
        -----------
        args : argparse.Namespace
            The parsed command-line arguments.
        """
        if args.producers is not None:
            self.producers = args.producers

        if args.consumers is not None:
            self.consumers = args.consumers

        if args.bootstrap_server is not None:
            self.bootstrap_server = args.bootstrap_server

        if args.security_protocol is not None:
            self.security_protocol = args.security_protocol

        if args.ssl_check_hostname is not None:
            self.ssl_check_hostname = args.ssl_check_hostname

        if args.group is not None:
            self.group = args.group

        if args.topic is not None:
            self.topic = args.topic

        if args.acks is not None:
            self.acks = args.acks

    @property
    def acks(self) -> int | str:
        return self._acks

    @acks.setter
    def acks(self, acks) -> None:
        if not isinstance(acks, int) and not isinstance(acks, str):
            raise ValueError("Acks not of type str or int.")
        self._acks = acks

    @property
    def group(self) -> str:
        """
        Returns the Kafka consumer group ID.

        Returns:
        --------
        str
            The Kafka consumer group ID.
        """
        return self._group

    @property
    def topic(self) -> str:
        """
        Returns the Kafka topic.

        Returns:
        --------
        str
            The Kafka topic name.
        """
        return self._topic

    @group.setter
    def group(self, group: str) -> None:
        """
        Sets the Kafka consumer group ID after validating the input.

        Parameters:
        -----------
        group : str
            The Kafka consumer group ID.

        Raises:
        -------
        ValueError
            If the group ID is not a valid string.
        """
        if not isinstance(group, str) or not group.strip():
            raise ValueError("-G (--group) must be a non-empty string.")
        self._group = group

    @topic.setter
    def topic(self, topic: str) -> None:
        """
        Sets the Kafka topic after validating the input.

        Parameters:
        -----------
        topic : str
            The Kafka topic name.

        Raises:
        -------
        ValueError
            If the topic name is not a valid string.
        """
        if not isinstance(topic, str) or not topic.strip():
            raise ValueError("-T (--topic) must be a non-empty string.")
        self._topic = topic

    @property
    def producers(self) -> int:
        """
        Returns the number of producers.

        Returns:
        --------
        int
            The number of Kafka producers.
        """
        return self._producers

    @property
    def consumers(self) -> int:
        """
        Returns the number of consumers.

        Returns:
        --------
        int
            The number of Kafka consumers.
        """
        return self._consumers

    @property
    def bootstrap_server(self) -> str:
        """
        Returns the bootstrap server address.

        Returns:
        --------
        str
            The Kafka cluster address (bootstrap server).
        """
        return self._bootstrap_server

    @property
    def security_protocol(self) -> str:
        """
        Returns the security protocol used for communication.

        Returns:
        --------
        str
            The security protocol for Kafka communication (e.g., SSL, PLAINTEXT).
        """
        return self._security_protocol

    @property
    def ssl_check_hostname(self) -> bool:
        """
        Returns the SSL hostname verification flag.

        Returns:
        --------
        bool
            Whether the SSL hostname verification is enabled.
        """
        return self._ssl_check_hostname

    @producers.setter
    def producers(self, producers: int) -> None:
        """
        Sets the number of producers after validating the input.

        Parameters:
        -----------
        producers : int
            The number of producers. Must be a non-negative integer.

        Raises:
        -------
        ValueError
            If the input is not a non-negative integer.
        """
        if not isinstance(producers, int) or producers < 0:
            raise ValueError("-P (--producers) must be a non-negative integer.")
        self._producers = producers

    @consumers.setter
    def consumers(self, consumers: int) -> None:
        """
        Sets the number of consumers after validating the input.

        Parameters:
        -----------
        consumers : int
            The number of consumers. Must be a non-negative integer.

        Raises:
        -------
        ValueError
            If the input is not a non-negative integer.
        """
        if not isinstance(consumers, int) or consumers < 0:
            raise ValueError("-C (--consumers) must be a non-negative integer.")
        self._consumers = consumers

    @bootstrap_server.setter
    def bootstrap_server(self, bootstrap_server: str) -> None:
        """
        Sets the bootstrap server address after validating the input.

        Parameters:
        -----------
        bootstrap_server : str
            The Kafka cluster address.

        Raises:
        -------
        ValueError
            If the bootstrap server address is not a valid string.
        """
        if not isinstance(bootstrap_server, str) or not bootstrap_server.strip():
            raise ValueError("-BS (--bootstrap-server) must be a non-empty string.")
        self._bootstrap_server = bootstrap_server

    @security_protocol.setter
    def security_protocol(self, security_protocol: str) -> None:
        """
        Sets the security protocol after validating the input.

        Parameters:
        -----------
        security_protocol : str
            The security protocol (e.g., SSL, PLAINTEXT).

        Raises:
        -------
        ValueError
            If the security protocol is not a valid string.
        """
        if not isinstance(security_protocol, str) or not security_protocol.strip():
            raise ValueError("-SP (--security-protocol) must be a non-empty string.")
        self._security_protocol = security_protocol

    @ssl_check_hostname.setter
    def ssl_check_hostname(self, ssl_check_hostname: bool) -> None:
        """
        Sets the SSL hostname verification flag after validating the input.

        Parameters:
        -----------
        ssl_check_hostname : bool
            The SSL hostname verification flag.

        Raises:
        -------
        ValueError
            If the SSL check hostname flag is not a boolean.
        """
        if not isinstance(ssl_check_hostname, bool):
            raise ValueError("-SCHN (--ssl-check-hostname) must be a boolean.")
        self._ssl_check_hostname = ssl_check_hostname
