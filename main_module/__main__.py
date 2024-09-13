"""
Main file that runs the overall logic
"""

import subprocess
import os
from python_kafka.core.cli.parse import CLIOptions


def spawn_producer_container(producers: int) -> tuple[bool, Exception]:
    """
    Spawn the producer Docker container
    """
    try:
        subprocess.run(
            [
                "docker",
                "build",
                "-t",
                "producer-container",
                os.path.join(
                    os.path.dirname(os.path.dirname(__file__)),
                    "build/containers/producers/.",
                ),
            ],
            check=True,
        )
        subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "producer-container",
                "--network",
                "kafka_network",
                "-e",
                "PRODUCERS",
                str(producers),
            ],
            check=True,
        )
    except Exception as exc:  # pylint: disable=W0718
        return False, exc

    return True, None


def spawn_consumer_container(consumers: int) -> tuple[bool, Exception]:
    """
    Spawn the consumer Docker container
    """
    try:
        subprocess.run(
            [
                "docker",
                "build",
                "-t",
                "consumer-container",
                os.path.join(
                    os.path.dirname(os.path.dirname(__file__)),
                    "build/containers/consumers/.",
                ),
            ],
            check=True,
        )
        subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "consumer-container",
                "--network",
                "kafka_network",
                "-e",
                "CONSUMERS",
                str(consumers),
            ],
            check=True,
        )
    except Exception as exc:  # pylint: disable=W0718
        return False, exc

    return True, None


def main():
    """
    Explanation:
        The main method is responsible for generating messages, creating consumers, preparing
        the threads to poll for messages, and scheduling the async logging functions.
    """

    cli = CLIOptions()

    success, exc = spawn_producer_container(cli.producers)

    if not success:
        if exc:
            raise exc
        print("An error occurred in spawn_producer_container")

    success, exc = spawn_consumer_container(cli.consumers)

    if not success:
        if exc:
            raise exc
        print("An error occurred in spawn_consumer_container")


if __name__ == "__main__":
    main()
