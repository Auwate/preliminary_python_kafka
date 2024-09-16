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
import docker.models.containers
from docker.models.images import Image
import docker.models.images
import docker.models.volumes
import docker.types
from python_kafka.core.kafka.admin.admin_builder import AdminBuilder
from python_kafka.core.kafka.admin.admin import Admin
from python_kafka.core.cli.parse import CLIOptions

def build_image(
    client: docker.DockerClient,
    path: str,
    dockerfile: str,
    tag: str
) -> tuple[Image, Exception]:
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
        return None, exc

    return image, None

def spawn_containers(
    client: docker.DockerClient,
    image: Image,
    network: str,
    environment_variables: dict[str, str | int | bool]
) -> tuple[Container, Exception]:
    try:
        container: Container = client.containers.run(
            image=image,
            network=network,
            environment=environment_variables,
            detach=True,
            mounts=[docker.types.Mount(source=os.path.join(os.path.dirname(os.path.dirname(__file__)), "secrets"), target="/home/program/preliminary_python_kafka/secrets/", type="bind")],
        )
    except Exception as exc:  # pylint: disable=W0718
        return None, exc
    return container, None

def setup_topics(
    topic_name: str,
    num_partitions: int,
    admin: Admin
) -> Exception:
    if admin.get_topic_details(topic_name) is None:
        success, exc = admin.create_topic(topic_name=topic_name, num_partitions=num_partitions, repli_factor=1, timeout_ms=1000)
        if not success:
            return exc
    return None

async def stop_containers(
    container: Container,
) -> Exception:
    try:
        await container.stop(30)
    except Exception as exc:  # pylint: disable=W0718
        return exc
    return None

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

def gather_logs(
    container: Container
) -> tuple[str, Exception]:
    try:
        logs = container.logs().decode()
    except docker.errors.APIError as exc:
        return None, exc
    return logs, None

def delete_containers(
    container: Container
) -> Exception:
    try:
        if container.status == "exited":
            raise docker.errors.APIError("Container had already exited before deleting.")
        container.remove(v=True)
    except docker.errors.APIError as exc:
        return exc
    return None

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
    admin_client: Admin = (
        AdminBuilder()
            .bootstrap_servers(cli.bootstrap_server)
            .security_protocol(cli.security_protocol)
            .ssl_check_hostname(cli.ssl_check_hostname)
            .build()
    )
    consumer_tag = "consumer-image"
    producer_tag = "producer-image"

    print(f"\nINFO: {datetime.datetime.now()}: Start of logs...\n------\n")

    producer_image, exc = check_image_exists(client, producer_tag)

    if not producer_image:

        if exc:
            print(f"\nERROR: {datetime.datetime.now()}: An error occurred in check_image_exists for producer image\n")
            raise exc

        print(f"\nINFO: {datetime.datetime.now()}: Building producer image...\n")

        producer_image, exc = build_image(
            client,
            os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "build/containers/producers/",
            ),
            "Dockerfile",
            "producer-image"
        )

        if exc:
            print(f"\nERROR: {datetime.datetime.now()}: An error occurred in build_image for producer image\n")
            raise exc

    consumer_image, exc = check_image_exists(client, consumer_tag)

    if not consumer_image:

        if exc:
            print(f"\nERROR: {datetime.datetime.now()}: An error occurred in check_image_exists for consumer image\n")
            raise exc

        print(f"\nINFO: {datetime.datetime.now()}: Building consumer image...\n")

        consumer_image, exc = build_image(
            client,
            os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "build/containers/consumers/",
            ),
            "Dockerfile",
            "consumer-image"
        )

        if exc:
            print(f"\nERROR: {datetime.datetime.now()}: An error occurred in build_image for consumer image\n")
            raise exc

    print(f"\nINFO: {datetime.datetime.now()}: Setting up topic in Kafka cluster...\n")

    exc: Exception = setup_topics("TEST", cli.consumers, admin_client)

    if exc:
        print(f"\nERROR: {datetime.datetime.now()}: An error occurred in setup_topics.\n")
        raise exc

    print(f"\nINFO: {datetime.datetime.now()}: Starting containers...\n")

    print(f"\nINFO: {datetime.datetime.now()}: 1: Producer...\n")

    producer_container, exc = spawn_containers(client, producer_image, "preliminary_python_kafka_kafka_network", env_args)

    if exc:
        print(f"\nERROR: {datetime.datetime.now()}: An error occurred in spawn_containers for producer container\n")
        raise exc

    time.sleep(30)

    print(f"\nINFO: {datetime.datetime.now()}: 2: Consumer...\n")

    consumer_container, exc = spawn_containers(client, consumer_image, "preliminary_python_kafka_kafka_network", env_args)

    if exc:
        print(f"\nERROR: {datetime.datetime.now()}: An error occurred in spawn_container for consumer container\n")
        raise exc

    time.sleep(30)

    print(f"\nINFO: {datetime.datetime.now()}: Waiting 60 seconds for results...\n")

    time.sleep(20)

    print(f"\nINFO: {datetime.datetime.now()}: Sending terminate signal...\n")

    tasks: list[asyncio.Task] = [
        asyncio.create_task(stop_containers(producer_container)),
        asyncio.create_task(stop_containers(consumer_container))
    ]

    await asyncio.gather(*tasks)

    print(f"\nINFO: {datetime.datetime.now()}: Gathering diagnostics...\n")

    #producer_logs, exc = gather_logs(producer_container)

    # if exc:
    #     print(f"\nERROR: {datetime.datetime.now()}: An error occurred in gather_logs for producer container")
    #     raise exc

    consumer_logs, exc = gather_logs(consumer_container)

    if exc:
        print(f"\nERROR: {datetime.datetime.now()}: An error occurred in gather_logs for consumer container\n")
        raise exc
    print(consumer_logs)

    print(f"\nINFO: {datetime.datetime.now()}: Deleting containers...\n")

    exc = delete_containers(producer_container)

    if exc:
        print(f"\nERROR: {datetime.datetime.now()}: An error occurred in delete_containers for producer container\n")
        raise exc

    exc = delete_containers(consumer_container)

    if exc:
        print(f"\nERROR: {datetime.datetime.now()}: An error occurred in delete_containers for consumer container\n")
        raise exc

    print(f"\nINFO: {datetime.datetime.now()}: Producer results:\n--------\n")

    # print(producer_logs)

    print(f"\nINFO: {datetime.datetime.now()}: Consumer results:\n--------\n")

    print(consumer_logs)

    print(f"\nINFO: {datetime.datetime.now()}: End of logs.\n")

if __name__ == "__main__":
    asyncio.run(main())
