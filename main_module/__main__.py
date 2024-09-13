"""
Main file that runs the overall logic
"""

import os
import docker
from python_kafka.core.cli.parse import CLIOptions

def build_image(
    client: docker.DockerClient,
    path: str,
    dockerfile: str,
    tag: str
) -> tuple[bool, str, Exception]:
    try:
        image, logs = client.images.build(
            path = path,
            dockerfile = dockerfile,
            tag=tag
        )

        for n in logs:
            print(n)

    except Exception as exc:  # pylint: disable=W0718
        return False, None, exc

    return True, image, None

def spawn_producer_container(
    client: docker.DockerClient, producers: int, image: str
) -> tuple[bool, Exception]:
    """
    Spawn the producer Docker container
    """
    try:
        client.containers.run(
            image=image,
            network="preliminary_python_kafka_kafka_network",
            environment={"PRODUCERS": str(producers)},
        )

    except Exception as exc:  # pylint: disable=W0718
        return False, exc

    return True, None


def spawn_consumer_container(
    client: docker.DockerClient, consumers: int, image: str
) -> tuple[bool, Exception]:
    """
    Spawn the consumer Docker container
    """
    try:

        client.containers.run(
            image=image,
            network="kafka_network",
            environment={"PRODUCERS": str(consumers)},
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
    client = docker.from_env()

    print("Producer Logs\n------\n")

    success, image, exc = build_image(
        client,
        os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "build/containers/producers/",
        ),
        "Dockerfile",
        "producer-container"
    )

    #success, exc = spawn_producer_container(client, cli.producers, image)

    if not success:
        if exc:
            raise exc
        print("An error occurred in spawn_producer_container")

    print("Consumer Logs\n------\n")

    success, image, exc = build_image(
        client,
        os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "build/containers/consumers/",
        ),
        "Dockerfile",
        "consumer-container"
    )

    #success, exc = spawn_consumer_container(client, cli.consumers)

    if not success:
        if exc:
            raise exc
        print("An error occurred in spawn_consumer_container")

    print("Results\n------\n")


if __name__ == "__main__":
    main()
