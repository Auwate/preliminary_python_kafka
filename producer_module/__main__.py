"""
The main module used by the producer container.

It uses poetry run python3 -m producer_module
"""

import os
import asyncio
import signal
import datetime
import time
from concurrent.futures import ThreadPoolExecutor
from python_kafka.core.kafka.producer.producer import Producer
from python_kafka.core.kafka.producer.producer_builder import ProducerBuilder


async def handle_sigterm(sig: signal.Signals) -> None:
    """
    Asynchronously handles the SIGTERM signal for graceful shutdown of the producer.

    This function:
    1. Logs the signal received and pauses for 4 seconds to allow in-flight operations to complete.
    2. Marks all Kafka producers for shutdown by setting their `shutdown` attribute to True.
    3. Awaits the completion of all active producer tasks using asyncio's `gather`.
    4. Aggregates the total number of messages successfully sent by all tasks.
    5. Stops the asyncio event loop once all tasks are completed.

    Args:
        sig (signal.Signals): The signal received (e.g., SIGTERM or SIGINT).
    """
    print(f"\nINFO: {datetime.datetime.now()}: SIGNAL {sig} received...\n", flush=True)

    # Wait for any in-progress operations to settle
    time.sleep(4)

    # Mark all Kafka producers for shutdown
    for n in producer_list:
        n.shutdown = True

    # Await completion of all producer tasks
    await asyncio.gather(*tasks)

    amount_sent = 0
    amount_per_second = 0

    total_sent = 0
    total_per_second = 0

    try:
        # Aggregate the total number of messages sent
        for task in tasks:
            amount_sent, amount_per_second = task.result()
            total_sent += amount_sent
            total_per_second += amount_per_second
    except Exception as exc:
        raise exc

    print(
        f"\nINFO: {datetime.datetime.now()}: Amount sent - {total_sent}\n",
        f"\nINFO: {datetime.datetime.now()}: Average sent per second -",
        f"{total_per_second // len(producer_list)}\n"
        flush=True,
    )

    # Final pause before shutdown
    time.sleep(4)

    # Stop the asyncio event loop
    asyncio.get_event_loop().stop()


# Global lists to keep track of active Kafka producers and asyncio tasks
producer_list: list[Producer] = []
tasks: list[asyncio.Task] = []


async def main():
    """
    Main function that initializes the Kafka producers and sets up the signal handlers.

    This function:
    1. Sets up signal handlers for SIGTERM and SIGINT to trigger a graceful shutdown.
    2. Retrieves Kafka connection and configuration settings from environment variables.
    3. Creates the specified number of producers and starts asynchronous tasks for message sending using a ThreadPoolExecutor.

    The producers are built using `ProducerBuilder` with settings such as `bootstrap_servers`,
    `security_protocol`, `ssl_check_hostname`, `acks`, and `topic`.
    """

    # Get the current event loop
    loop = asyncio.get_event_loop()

    # Add signal handlers for graceful shutdown on SIGTERM and SIGINT
    loop.add_signal_handler(
        signal.SIGTERM, lambda s=signal.SIGTERM: asyncio.create_task(handle_sigterm(s))
    )
    loop.add_signal_handler(
        signal.SIGINT, lambda s=signal.SIGINT: asyncio.create_task(handle_sigterm(s))
    )

    # Kafka configuration fetched from environment variables
    bootstrap_servers: str = os.environ["BOOTSTRAP_SERVERS"]
    security_protocol: str = os.environ["SECURITY_PROTOCOL"]
    ssl_check_hostname: bool = os.environ["SSL_CHECK_HOSTNAME"]

    # Number of Kafka producers to instantiate
    producers: int = int(os.environ["PRODUCERS"])

    # Kafka topic to which the producers will send messages
    topic: str = os.environ["TOPIC"]

    # Number of worker threads for the ThreadPoolExecutor
    workers: int = int(os.environ["WORKERS"])

    # Ensure SSL check hostname is properly set as a boolean
    ssl_check_hostname = ssl_check_hostname != "False"

    # Thread pool executor to handle concurrent message sending
    executor = ThreadPoolExecutor(max_workers=workers)

    # Create the specified number of producers
    for _ in range(producers):
        try:
            # Build a Kafka producer with the provided configuration
            producer: Producer = (
                ProducerBuilder()
                .bootstrap_servers(bootstrap_servers)
                .security_protocol(security_protocol)
                .ssl_check_hostname(ssl_check_hostname)
                .topic(topic)
                .build()
            )

            # Create an asyncio task for each producer to send messages concurrently
            tasks.append(asyncio.create_task(coro=producer.send_messages(executor)))

            # Keep track of the producer in the global list
            producer_list.append(producer)
        except Exception as exc:
            raise exc


if __name__ == "__main__":
    # Create a new asyncio event loop
    main_loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()

    try:
        # Schedule the main coroutine
        main_loop.create_task(coro=main())

        # Run the event loop indefinitely until a termination signal is received
        main_loop.run_forever()
    except KeyboardInterrupt:
        print(
            "Interrupted.", flush=True
        )  # Gracefully handle keyboard interrupt (Ctrl+C)
    except Exception as ex:
        raise ex  # Raise any other exceptions that occur during execution
    finally:
        # Close the event loop once done
        main_loop.close()
