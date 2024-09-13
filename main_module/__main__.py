"""
Main file that runs the overall logic
"""

import os
import docker
import time
import asyncio

from docker.models.containers import Container
from docker.models.images import Image
from python_kafka.core.cli.parse import CLIOptions

def build_image(
    client: docker.DockerClient,
    path: str,
    dockerfile: str,
    tag: str
) -> tuple[bool, Image, Exception]:
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

def spawn_containers(
    client: docker.DockerClient,
    image: Image,
    network: str,
    environment_variables: dict[str, str | int | bool]
) -> tuple[bool, Container, Exception]:
    try:
        container: Container = client.containers.run(
            image=image,
            network=network,
            environment=environment_variables
        )
    except Exception as exc:  # pylint: disable=W0718
        return False, None, exc
    return True, container, None
    

async def stop_containers(
    client: docker.DockerClient, container: Container
) -> tuple[bool, Exception]:
    try:
        await container.stop(30)
    except Exception as exc:  # pylint: disable=W0718
        return False, exc
    return True, None


async def main():
    """
    Explanation:
        The main method is responsible for generating messages, creating consumers, preparing
        the threads to poll for messages, and scheduling the async logging functions.
    """

    cli = CLIOptions()
    env_args: dict[str, str | int | bool] = {
        "AMOUNT": 0,
        "BOOTSTRAP_SERVERS": cli.bootstrap_server,
        "SECURITY_PROTOCOL": cli.security_protocol,
        "SSL_CHECK_HOSTNAME": cli.ssl_check_hostname
    }
    client = docker.from_env()

    print("\nStart of logs...\n------\n")

    print("\nBuilding images...\n")

    print("\n1: Producer...")

    success, producer_image, exc = build_image(
        client,
        os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "build/containers/producers/",
        ),
        "Dockerfile",
        "producer-image"
    )

    print("\n2: Consumer...")

    success, consumer_image, exc = build_image(
        client,
        os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "build/containers/consumers/",
        ),
        "Dockerfile",
        "consumer-image"
    )

    print("\nStarting containers...\n")

    print("\n1: Producer...\n")

    env_args["AMOUNT"] = cli.producers
    success, producer_container, exc = spawn_containers(client, producer_image, "preliminary_python_kafka_kafka_network", env_args)

    if not success:
        if exc:
            raise exc
        print("An error occurred in spawn_producer_container")

    print("\n2: Consumer...\n")

    env_args["AMOUNT"] = cli.consumers
    success, consumer_container, exc = spawn_containers(client, consumer_image, "preliminary_python_kafka_kafka_network", env_args)

    if not success:
        if exc:
            raise exc
        print("An error occurred in spawn_consumer_container")

    print("\nWaiting 60 seconds for results...\n")

    time.sleep(60)

    print("\nSending terminate signal...\n")

    tasks: list[asyncio.Task] = [
        asyncio.create_task(stop_containers(producer_container)),
        asyncio.create_task(stop_containers(consumer_container))
    ]

    await asyncio.gather(*tasks)

    print("\nTerminated...")

    print("\nProducer results:\n--------\n")

    print(producer_container.logs())

    print("\nConsumer results:\n--------\n")

    print(consumer_container.logs())

    print("\nEnd of logs.")

if __name__ == "__main__":
    asyncio.run(main())
