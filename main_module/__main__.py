"""
Main file that runs the overall logic
"""

import os
import docker
import time
import datetime
import asyncio

import docker.errors
import docker.models
from docker.models.containers import Container
from docker.models.images import Image
import docker.models.images
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
            tag=tag,
            rm=True
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
            environment=environment_variables,
            detach=True,
            auto_remove=True
        )
    except Exception as exc:  # pylint: disable=W0718
        return False, None, exc
    return True, container, None
    

async def stop_containers(
    container: Container
) -> tuple[bool, Exception]:
    try:
        container.stop(30)
    except Exception as exc:  # pylint: disable=W0718
        return False, exc
    return True, None

def check_image_exists(
    client: docker.DockerClient,
    tag: str
) -> tuple[Image, Exception]:
    try:
        image: Image = client.images.get(tag)
    except docker.errors.ImageNotFound:
        return None, None
    except docker.errors.APIError as exc:
        return None, exc
    return image, None

async def main():
    """
    Explanation:
        The main method is responsible for generating messages, creating consumers, preparing
        the threads to poll for messages, and scheduling the async logging functions.
    """

    cli = CLIOptions()
    env_args: dict[str, str | int | bool] = {
        "PRODUCERS": cli.producers,
        "CONSUMERS": cli.consumers,
        "GROUP": cli.group,
        "TOPIC": cli.topic,
        "BOOTSTRAP_SERVERS": cli.bootstrap_server,
        "SECURITY_PROTOCOL": cli.security_protocol,
        "SSL_CHECK_HOSTNAME": cli.ssl_check_hostname
    }
    client = docker.from_env()
    consumer_tag = "consumer-image"
    producer_tag = "producer-image"

    print(f"\nINFO: {datetime.datetime.now()}: Start of logs...\n------\n")

    producer_image, exc = check_image_exists(client, producer_tag)

    if not producer_image:

        if not exc:
            raise exc

        print(f"\nINFO: {datetime.datetime.now()}: Building producer image...\n")

        success, producer_image, exc = build_image(
            client,
            os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "build/containers/producers/",
            ),
            "Dockerfile",
            "producer-image"
        )

    consumer_image, exc = check_image_exists(client, consumer_tag)

    if not consumer_image:

        if exc:
            raise exc

        print(f"\nINFO: {datetime.datetime.now()}: Building consumer image...\n")

        success, consumer_image, exc = build_image(
            client,
            os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "build/containers/consumers/",
            ),
            "Dockerfile",
            "consumer-image"
        )

    print(f"\nINFO: {datetime.datetime.now()}: Starting containers...\n")

    print(f"\nINFO: {datetime.datetime.now()}: 1: Producer...\n")

    success, producer_container, exc = spawn_containers(client, producer_image, "preliminary_python_kafka_kafka_network", env_args)

    if not success:
        print(f"\nERROR: {datetime.datetime.now()}: An error occurred in spawn_producer_container")
        if exc:
            raise exc

    print(f"\nINFO: {datetime.datetime.now()}: 2: Consumer...\n")

    success, consumer_container, exc = spawn_containers(client, consumer_image, "preliminary_python_kafka_kafka_network", env_args)

    if not success:
        print(f"\nERROR: {datetime.datetime.now()}: An error occurred in spawn_consumer_container")
        if exc:
            raise exc

    print(f"\nINFO: {datetime.datetime.now()}: Waiting 60 seconds for results...\n")

    time.sleep(5)

    print(f"\nINFO: {datetime.datetime.now()}: Sending terminate signal...\n")

    tasks: list[asyncio.Task] = [
        asyncio.create_task(stop_containers(producer_container)),
        asyncio.create_task(stop_containers(consumer_container))
    ]

    await asyncio.gather(*tasks)

    print(f"\nINFO: {datetime.datetime.now()}: Terminated...\n")

    print(f"\nINFO: {datetime.datetime.now()}: Producer results:\n--------\n")

    print(producer_container.logs().decode())

    print(f"\nINFO: {datetime.datetime.now()}: Consumer results:\n--------\n")

    print(consumer_container.logs().decode())

    print(f"\nINFO: {datetime.datetime.now()}: End of logs.")

if __name__ == "__main__":
    asyncio.run(main())
