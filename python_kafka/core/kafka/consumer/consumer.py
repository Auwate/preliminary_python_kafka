"""
A class for consuming messages from Kafka using the KafkaConsumer.

This class initializes a Kafka consumer with the specified configuration and provides
methods for consuming messages asynchronously and logging them.

Attributes:
-----------
_consumer : KafkaConsumer
    An instance of the KafkaConsumer configured with the provided settings.
_queue : asyncio.Queue
    An asyncio queue where consumed messages are placed.

Methods:
--------
consumer() -> KafkaConsumer
    Provides access to the underlying KafkaConsumer instance.

queue() -> asyncio.Queue
    Provides access to the asyncio queue used by this Consumer.

consume_messages(timeout: int, max_records: int) -> tuple[bool, Exception]
    Consumes messages from Kafka and puts them into an asyncio.Queue.

    Parameters:
    -----------
    timeout : int
        The maximum amount of time (in milliseconds) to wait for messages.
    max_records : int
        The maximum number of records to fetch in one poll.

    Returns:
    --------
    tuple[bool, Exception]:
        A tuple where the first element is a boolean indicating whether all messages were
        consumed and the second element is an Exception which will be None if there was
        no error.

    Raises:
    -------
    Exception:
        Any exception raised during the message consumption process will be propagated.

    Notes:
    -------
    This method runs an infinite loop to poll messages from Kafka and put them into
    the provided asyncio.Queue. Exceptions during this process are raised to be
    handled by the caller.

logger(log_obj: Logger, timeout: int)
    An asynchronous function that consumes data from the Queue and sends it to the logger.

    Parameters:
    -----------
    log_obj : Logger
        A singleton logging object used to log the consumed messages.
    timeout : int
        The maximum amount of time (in seconds) to wait for messages from the queue.

    Raises:
    -------
    TimeoutError:
        If the timeout for waiting on the queue is reached.
    Exception:
        Any exception raised during the logging process will be propagated.

    Notes:
    -------
    This method runs an infinite loop to retrieve messages from the queue and pass them to
    the logging object. If a message is not received within the specified timeout, a
    TimeoutError is raised.
"""

import asyncio
import datetime
import random
from kafka import KafkaConsumer
from ....configs.configs import ssl_cafile, ssl_certfile, ssl_keyfile, ssl_password

class Consumer:
    """
    A class for consuming messages from Kafka using the KafkaConsumer.

    This class sets up a Kafka consumer with the given configuration and provides methods
    for consuming messages asynchronously and simulating their processing.

    Attributes:
    -----------
    _consumer : KafkaConsumer
        An instance of the KafkaConsumer configured with the provided settings.
    _shutdown : bool
        A flag to signal when the consumer should stop consuming messages.

    Methods:
    --------
    __init__(bs_servers: str, sec_protocol: str, ssl_check_hostname: bool, topic: str, group: str)
        Initializes the Consumer with the given Kafka configuration settings.
    """

    def __init__(self, bs_servers: str, sec_protocol: str, ssl_check_hostname: bool, topic: str, group: str) -> None:
        """
        Initializes the Consumer with the provided configuration settings.

        Parameters:
        -----------
        bs_servers : str
            The Kafka bootstrap servers to connect to.
        sec_protocol : str
            The security protocol to use (e.g., "SSL").
        ssl_check_hostname : bool
            Whether to check the hostname in SSL certificates.
        topic : str
            The Kafka topic to consume messages from.
        group : str
            The Kafka consumer group ID.

        Initializes the KafkaConsumer instance with the specified configuration.
        """
        self._shutdown = False
        self._consumer = KafkaConsumer(
            [topic],
            bootstrap_servers=bs_servers,
            group_id=group,
            security_protocol=sec_protocol,
            ssl_check_hostname=ssl_check_hostname,
            ssl_cafile=ssl_cafile,
            ssl_certfile=ssl_certfile,
            ssl_keyfile=ssl_keyfile,
            ssl_password=ssl_password,
        )

    @property
    def shutdown(self) -> bool:
        """
        Returns the current state of the shutdown flag.

        Returns:
        --------
        bool:
            True if the consumer is in shutdown mode, otherwise False.
        """
        return self._shutdown

    @shutdown.setter
    def shutdown(self, shutdown: bool) -> None:
        """
        Sets the shutdown flag to control the consumer's lifecycle.

        Parameters:
        -----------
        shutdown : bool
            True to signal the consumer to shut down, False to allow consumption.

        Raises:
        -------
        ValueError:
            If the provided shutdown value is not a boolean.
        """
        if not isinstance(shutdown, bool):
            raise ValueError("Shutdown flag must be a boolean value.")
        self._shutdown = shutdown

    @property
    def consumer(self) -> KafkaConsumer:
        """
        Provides access to the underlying KafkaConsumer instance.

        Returns:
        --------
        KafkaConsumer:
            The KafkaConsumer instance used by this Consumer.
        """
        return self._consumer

    def consume_messages(self, timeout: int, max_records: int) -> int:
        """
        Consumes messages from Kafka and simulates logging or processing in an infinite loop.

        Parameters:
        -----------
        timeout : int
            The maximum amount of time (in milliseconds) to wait for messages.
        max_records : int
            The maximum number of records to fetch in one poll.

        Returns:
        --------
        int:
            The total number of consumed messages.

        Raises:
        -------
        Exception:
            Any exception raised during the message consumption process will be logged and propagated.

        Notes:
        -------
        This method simulates processing (e.g., logging or database transactions) by 
        adding a delay proportional to the number of messages fetched. The loop stops when
        the shutdown flag is set to True.
        """
        consumed = 0
        while True:
            if self.shutdown:
                return consumed
            try:
                data = self.consumer.poll(timeout_ms=timeout, max_records=max_records)
                if data:
                    # Simulate processing delay for each batch of messages
                    asyncio.sleep(len(data) * 0.005 * random.randint(1, 10))
                    print(data)
                    consumed += len(data)
            except Exception as exc:  # pylint: disable=W0718
                print(f"\nERROR: {datetime.datetime.now()}: {exc}\n")
