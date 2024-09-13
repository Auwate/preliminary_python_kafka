"""
A command-line interface for configuring Kafka producers and consumers.

This module provides functionality to create a command-line argument parser and
to process the arguments for setting up the number of Kafka producers and consumers.

The `CLIOptions` class allows you to specify the amount of producers and consumers
through command-line arguments and ensures that these values are properly validated
and converted to integers.

Functions:
-----------
create_parser() -> argparse.ArgumentParser
    Creates and returns an argument parser for command-line arguments.

Classes:
---------
CLIOptions
    A class for handling command-line arguments for Kafka producers and consumers.

    Attributes:
    -----------
    _producers : int
        The number of Kafka producers (default: 0).
    _consumers : int
        The number of Kafka consumers (default: 0).

    Methods:
    --------
    __init__(parser: argparse.ArgumentParser = None, args: list[str] = None) -> None
        Initializes the CLIOptions instance and processes the provided arguments.

    read_args(parser: argparse.ArgumentParser, args: list[str]) -> None
        Reads and parses command-line arguments using the provided parser.

    evaluate_input(parser: argparse.ArgumentParser, args: list[str]) -> None
        Validates and sets the producer and consumer values from the parsed arguments.

    producers() -> int
        Returns the number of producers.

    consumers() -> int
        Returns the number of consumers.

    producers.setter(producers: str) -> None
        Sets the number of producers after validating the input.

    consumers.setter(consumers: str) -> None
        Sets the number of consumers after validating the input.
"""

import argparse


def create_parser() -> argparse.ArgumentParser:
    """
    Creates a command-line argument parser.

    Returns:
    --------
    argparse.ArgumentParser
        The argument parser with predefined arguments for producers and consumers.
    """
    parser = argparse.ArgumentParser(
        description="Configure Kafka producers and consumers."
    )

    parser.add_argument(
        "-P",
        "--producers",
        help="Type: int | Specify the number of producers that will send messages to Kafka.",
        type=int,
    )
    parser.add_argument(
        "-C",
        "--consumers",
        help="Type: int | Specify the number of consumers that will read messages from Kafka. "
        "This also affects the number of loggers, as consumers log information.",
        type=int,
    )

    return parser


class CLIOptions:
    """
    A class to handle and validate command-line arguments for Kafka producers and consumers.

    Attributes:
    -----------
    _producers : int
        The number of Kafka producers.
    _consumers : int
        The number of Kafka consumers.

    Methods:
    --------
    __init__(parser: argparse.ArgumentParser = None, args: list[str] = None) -> None
        Initializes the CLIOptions instance. Processes the provided parser and arguments.

    read_args(parser: argparse.ArgumentParser, args: list[str]) -> None
        Reads and parses command-line arguments using the provided parser.

    evaluate_input(parser: argparse.ArgumentParser, args: list[str]) -> None
        Validates and assigns producer and consumer values from the parsed arguments.

    producers() -> int
        Returns the number of producers.

    consumers() -> int
        Returns the number of consumers.

    producers.setter(producers: str) -> None
        Sets the number of producers after validating the input.

    consumers.setter(consumers: str) -> None
        Sets the number of consumers after validating the input.
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
        self._consumers = 0
        self._producers = 0
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
        Validates and assigns the values of producers and consumers from the parsed arguments.

        Parameters:
        -----------
        parser : argparse.ArgumentParser
            The argument parser used.
        args : argparse.Namespace
            The parsed command-line arguments.
        """
        if args.producers is not None:
            self.producers = args.producers

        if args.consumers is not None:
            self.consumers = args.consumers

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

    @producers.setter
    def producers(self, producers: int) -> None:
        """
        Sets the number of producers after validating the input.

        Parameters:
        -----------
        producers : int
            The number of producers. Must be a positive integer.

        Raises:
        -------
        ValueError
            If the input is not a positive integer.
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
            The number of consumers. Must be a positive integer.

        Raises:
        -------
        ValueError
            If the input is not a positive integer.
        """
        if not isinstance(consumers, int) or consumers < 0:
            raise ValueError("-C (--consumers) must be a non-negative integer.")
        self._consumers = consumers
