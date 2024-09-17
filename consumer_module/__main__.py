"""
The main module used by the consumer container.

It uses poetry run python3 -m consumer_module
"""

import os
import asyncio
import signal
import datetime
import time
from kafka import TopicPartition
from python_kafka.core.kafka.consumer.consumer_builder import ConsumerBuilder
from python_kafka.core.kafka.consumer.consumer import Consumer


async def handle_sigterm(sig: signal.Signals) -> None:
    """
    Asynchronously handles the SIGTERM signal, which is triggered when the application is asked to terminate.
    
    This function:
    1. Logs the received signal and sleeps for 4 seconds to allow for any ongoing operations to settle.
    2. Signals all active Kafka consumers to stop by setting their `shutdown` attribute to True.
    3. Waits for the active consumption tasks to complete using asyncio's `gather`.
    4. Aggregates the total number of consumed messages from all tasks.
    5. Stops the asyncio event loop once all tasks are completed.

    Args:
        sig (signal.Signals): The signal received by the application (e.g., SIGTERM or SIGINT).
    """
    print(f"\nINFO: {datetime.datetime.now()}: SIGNAL {sig} received...\n", flush=True)

    # Wait for any in-progress tasks to finish gracefully
    time.sleep(4)

    # Mark all Kafka consumers for shutdown
    for n in consumer_list:
        n.shutdown = True

    # Await completion of all tasks
    await asyncio.gather(*tasks)

    amount_consumed = 0

    try:
        # Aggregate the total number of consumed messages
        for task in tasks:
            amount_consumed += task.result()
    except Exception as exc:  # pylint: disable=W0718
        raise exc

    print(
        f"\nINFO: {datetime.datetime.now()}: Amount consumed - {amount_consumed}\n",
        flush=True,
    )

    time.sleep(4)  # Another pause before shutting down

    # Stop the asyncio event loop
    asyncio.get_event_loop().stop()


# Global lists to manage active Kafka consumers and associated asyncio tasks
consumer_list: list[Consumer] = []
tasks: list[asyncio.Task] = []


async def main():
    """
    Main entry point for the consumer application.
    
    This function:
    1. Sets up signal handlers for SIGTERM and SIGINT to gracefully shut down the consumers.
    2. Retrieves Kafka connection and configuration details from environment variables.
    3. Initializes the specified number of Kafka consumers, assigns each to a topic partition, and starts consumption tasks.

    The Kafka consumers are created using the `ConsumerBuilder`, and each consumer task is asynchronously 
    scheduled to consume messages from its respective partition.
    """
    
    # Retrieve the current event loop
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

    # Ensure SSL check hostname is properly set as a boolean
    ssl_check_hostname = ssl_check_hostname != "False"

    # Retrieve the number of consumers, group, topic, and other settings from environment variables
    consumers: int = int(os.environ["CONSUMERS"])
    group: str = os.environ["GROUP"]
    topic: str = os.environ["TOPIC"]
    timeout: int = 100  # Timeout for consuming messages
    max_records = 100  # Maximum number of records to fetch per consumer poll

    # Create the specified number of Kafka consumers
    for i in range(consumers):
        try:
            # Build a Kafka consumer with the provided configuration
            consumer: Consumer = (
                ConsumerBuilder()
                .bootstrap_servers(bootstrap_servers)
                .security_protocol(security_protocol)
                .ssl_check_hostname(ssl_check_hostname)
                .group(group)
                .topic(None)  # Topic assignment is manual below
                .build()
            )
            
            # Manually assign the consumer to a specific partition of the topic
            consumer.consumer.assign([TopicPartition(topic, i)])
            
            # Create a task to asynchronously consume messages
            tasks.append(
                asyncio.create_task(
                    coro=consumer.consume_messages(
                        timeout=timeout, max_records=max_records
                    )
                )
            )
            
            # Keep track of the consumer in the global list
            consumer_list.append(consumer)
        except Exception as exc:  # pylint: disable=W0718
            raise exc


if __name__ == "__main__":
    """
    Entry point for the script when executed directly. Initializes the asyncio event loop, 
    schedules the main function, and keeps the event loop running until manually interrupted 
    or an exception occurs.
    """
    
    # Create a new asyncio event loop
    loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
    
    try:
        # Schedule the main coroutine
        loop.create_task(coro=main())
        
        # Run the event loop indefinitely until a termination signal is received
        loop.run_forever()
    except KeyboardInterrupt:
        print("Interrupted.", flush=True)  # Gracefully handle keyboard interrupt (Ctrl+C)
    except Exception as exc:
        raise exc  # Raise any other exceptions that occur during execution
    finally:
        # Close the event loop once done
        loop.close()
