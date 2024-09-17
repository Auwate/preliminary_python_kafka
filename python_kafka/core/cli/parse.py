"""
Argument parser. Uses Python's argparse library
"""

import argparse


def create_parser() -> argparse.ArgumentParser:
    """
    Creates and configures an argument parser for the CLI.

    Returns:
        argparse.ArgumentParser: Configured argument parser with various options
    """
    parser = argparse.ArgumentParser(
        description="Configure Kafka producers, consumers, and other settings."
    )

    # Argument for the number of Kafka producers
    parser.add_argument(
        "-P",
        "--producers",
        help="Type: int | Specify the number of producers that will send messages to Kafka.",
        type=int,
        required=True,
    )

    # Argument for the number of Kafka consumers
    parser.add_argument(
        "-C",
        "--consumers",
        help="Type: int | Specify the number of consumers that will read messages from Kafka.",
        type=int,
        required=True,
    )

    # Argument for the Kafka bootstrap server address
    parser.add_argument(
        "-BS",
        "--bootstrap-server",
        help="Type: str (Optional) The location of the Kafka cluster (default: localhost:9092).",
        type=str,
    )

    # Argument for the Kafka security protocol
    parser.add_argument(
        "-SP",
        "--security-protocol",
        help="Type: str (Optional) The security protocol for Kafka communication (default: SSL).",
        type=str,
    )

    # Argument for enabling SSL hostname verification
    parser.add_argument(
        "-SCHN",
        "--ssl-check-hostname",
        help="Type: bool (Optional) A flag to enable SSL hostname verification (default: False).",
        type=bool,
    )

    # Argument for the Kafka consumer group ID
    parser.add_argument(
        "-G",
        "--group",
        help="Type: str (Optional) The Kafka consumer group ID (default: Test Group).",
        type=str,
    )

    # Argument for the Kafka topic
    parser.add_argument(
        "-T",
        "--topic",
        help="Type: str (Optional) The Kafka topic (default: Test Topic).",
        type=str,
    )

    # Argument for the Kafka acknowledgment pattern
    parser.add_argument(
        "-A",
        "--acks",
        help="Type: int | str (Optional) The acknowledgement pattern between Kafka and producer. "
        "Values can be 0, 1 or 'all'.",
        type=str,
    )

    # Argument for the number of worker threads
    parser.add_argument(
        "-W",
        "--workers",
        help="Type: int (Optional) The amount of worker threads that will handle blocking code.",
        type=str,
    )

    return parser


class CLIOptions:  # pylint: disable=R0902
    """
    Manages command-line interface (CLI) options for Kafka configurations.

    Initializes default values and parses arguments using the provided parser.
    """

    def __init__(
        self, parser: argparse.ArgumentParser = None, args: list[str] = None
    ) -> None:
        """
        Initializes CLIOptions with default values or provided arguments.

        Parameters:
            parser (argparse.ArgumentParser, optional): The argument parser instance.
            args (list[str], optional): The list of command-line arguments to parse.
        """
        self._consumers = 5  # Default values
        self._producers = 5
        self._group = "Test_Group"
        self._topic = "Test_Topic"
        self._acks: str = 0  # Values can be 0, 1, or "all"
        self._workers: int = 10  # Number of worker threads
        self._bootstrap_server = "localhost:9092"
        self._security_protocol = "SSL"
        self._ssl_check_hostname = False
        self.read_args(parser, args)

    def read_args(self, parser: argparse.ArgumentParser, args: list[str]) -> None:
        """
        Parses and evaluates command-line arguments.

        Parameters:
            parser (argparse.ArgumentParser): The argument parser instance.
            args (list[str]): The list of command-line arguments to parse.
        """
        if not parser:
            parser = create_parser()
        arguments = parser.parse_args(args)
        self.evaluate_input(arguments)

    def evaluate_input(self, args: argparse.Namespace) -> None:
        """
        Evaluates and assigns parsed arguments to class attributes.

        Parameters:
            args (argparse.Namespace): The parsed command-line arguments.
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

        if args.workers is not None:
            self.workers = args.workers

    @property
    def workers(self) -> int:
        """
        Returns the number of worker threads.

        Returns:
            int: The number of worker threads.
        """
        return self._workers

    @workers.setter
    def workers(self, workers: int) -> None:
        """
        Sets the number of worker threads after validating the input.

        Parameters:
            workers (int): The number of worker threads.

        Raises:
            ValueError: If workers is not a positive integer.
        """
        if not str.isdigit(workers):
            raise ValueError("-W (--workers) must be a number")
        self._workers = int(workers)

    @property
    def acks(self) -> str:
        """
        Returns the acknowledgment pattern for Kafka producers.

        Returns:
            str: The acknowledgment pattern.
        """
        return self._acks

    @acks.setter
    def acks(self, acks) -> None:
        """
        Sets the acknowledgment pattern after validating the input.

        Parameters:
            acks (str): The acknowledgment pattern (0, 1, or 'all').

        Raises:
            ValueError: If acks is not valid.
        """
        if not isinstance(acks, str):
            raise ValueError("-A (--acks) must be 0, 1, or all.")
        else:
            self._acks = acks

    @property
    def group(self) -> str:
        """
        Returns the Kafka consumer group ID.

        Returns:
            str: The Kafka consumer group ID.
        """
        return self._group

    @property
    def topic(self) -> str:
        """
        Returns the Kafka topic.

        Returns:
            str: The Kafka topic name.
        """
        return self._topic

    @group.setter
    def group(self, group: str) -> None:
        """
        Sets the Kafka consumer group ID after validating the input.

        Parameters:
            group (str): The Kafka consumer group ID.

        Raises:
            ValueError: If the group ID is not a valid string.
        """
        if not isinstance(group, str) or not group.strip():
            raise ValueError("-G (--group) must be a non-empty string.")
        self._group = group

    @topic.setter
    def topic(self, topic: str) -> None:
        """
        Sets the Kafka topic after validating the input.

        Parameters:
            topic (str): The Kafka topic name.

        Raises:
            ValueError: If the topic name is not a valid string.
        """
        if not isinstance(topic, str) or not topic.strip():
            raise ValueError("-T (--topic) must be a non-empty string.")
        self._topic = topic

    @property
    def producers(self) -> int:
        """
        Returns the number of producers.

        Returns:
            int: The number of Kafka producers.
        """
        return self._producers

    @property
    def consumers(self) -> int:
        """
        Returns the number of consumers.

        Returns:
            int: The number of Kafka consumers.
        """
        return self._consumers

    @property
    def bootstrap_server(self) -> str:
        """
        Returns the bootstrap server address.

        Returns:
            str: The Kafka cluster address (bootstrap server).
        """
        return self._bootstrap_server

    @property
    def security_protocol(self) -> str:
        """
        Returns the security protocol used for communication.

        Returns:
            str: The security protocol for Kafka communication (e.g., SSL, PLAINTEXT).
        """
        return self._security_protocol

    @property
    def ssl_check_hostname(self) -> bool:
        """
        Returns the SSL hostname verification flag.

        Returns:
            bool: Whether the SSL hostname verification is enabled.
        """
        return self._ssl_check_hostname

    @producers.setter
    def producers(self, producers: int) -> None:
        """
        Sets the number of producers after validating the input.

        Parameters:
            producers (int): The number of producers. Must be a non-negative integer.

        Raises:
            ValueError: If the input is not a non-negative integer.
        """
        if not isinstance(producers, int) or producers < 0:
            raise ValueError("-P (--producers) must be a non-negative integer.")
        self._producers = producers

    @consumers.setter
    def consumers(self, consumers: int) -> None:
        """
        Sets the number of consumers after validating the input.

        Parameters:
            consumers (int): The number of consumers. Must be a non-negative integer.

        Raises:
            ValueError: If the input is not a non-negative integer.
        """
        if not isinstance(consumers, int) or consumers < 0:
            raise ValueError("-C (--consumers) must be a non-negative integer.")
        self._consumers = consumers

    @bootstrap_server.setter
    def bootstrap_server(self, bootstrap_server: str) -> None:
        """
        Sets the bootstrap server address after validating the input.

        Parameters:
            bootstrap_server (str): The Kafka cluster address.

        Raises:
            ValueError: If the bootstrap server address is not a valid string.
        """
        if not isinstance(bootstrap_server, str) or not bootstrap_server.strip():
            raise ValueError("-BS (--bootstrap-server) must be a non-empty string.")
        self._bootstrap_server = bootstrap_server

    @security_protocol.setter
    def security_protocol(self, security_protocol: str) -> None:
        """
        Sets the security protocol after validating the input.

        Parameters:
            security_protocol (str): The security protocol (e.g., SSL, PLAINTEXT).

        Raises:
            ValueError: If the security protocol is not a valid string.
        """
        if not isinstance(security_protocol, str) or not security_protocol.strip():
            raise ValueError("-SP (--security-protocol) must be a non-empty string.")
        self._security_protocol = security_protocol

    @ssl_check_hostname.setter
    def ssl_check_hostname(self, ssl_check_hostname: bool) -> None:
        """
        Sets the SSL hostname verification flag after validating the input.

        Parameters:
            ssl_check_hostname (bool): The SSL hostname verification flag.

        Raises:
            ValueError: If the SSL check hostname flag is not a boolean.
        """
        if not isinstance(ssl_check_hostname, bool):
            raise ValueError("-SCHN (--ssl-check-hostname) must be a boolean.")
        self._ssl_check_hostname = ssl_check_hostname
