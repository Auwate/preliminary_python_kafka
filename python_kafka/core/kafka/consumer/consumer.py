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
from kafka import KafkaConsumer
from ...logger import Logger
from ....configs.configs import ssl_cafile, ssl_certfile, ssl_keyfile, ssl_password


class Consumer:
    """
    A class for consuming messages from Kafka using the KafkaConsumer.

    This class sets up a Kafka consumer with the given configuration and provides methods
    for consuming messages asynchronously and logging them.

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

    def __init__(  # pylint: disable=R0913
        self,
        bs_servers: str,
        sec_protocol: str,
        ssl_check_hostname: bool,
        topic: str,
        group: str,
        queue: asyncio.Queue,
    ) -> None:
        """
        Initializes the Consumer with the given configuration.

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
        queue : asyncio.Queue
            The asyncio queue where consumed messages will be placed.

        Initializes the KafkaConsumer instance with the specified configuration.
        """
        self._queue = queue
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
    def queue(self) -> asyncio.Queue:
        """
        Provides access to the asyncio queue used by this Consumer.

        Returns:
        --------
        asyncio.Queue:
            The asyncio queue where consumed messages are placed.
        """
        return self._queue

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

    def consume_messages(
        self, timeout: int, max_records: int
    ) -> tuple[bool, Exception]:
        """
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
        """
        while True:
            data = self.consumer.poll(timeout_ms=timeout, max_records=max_records)
            if data:
                try:
                    asyncio.run_coroutine_threadsafe(
                        self.queue.put(data), loop=asyncio.get_running_loop()
                    )
                except Exception as exc:
                    raise exc

    async def logger(self, log_obj: Logger, timeout: int):
        """
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
        while True:
            try:
                data = await asyncio.wait_for(self.queue.get(), timeout=timeout)
            except TimeoutError as exc:
                raise TimeoutError("Timeout has been reached for logger") from exc
            except Exception as exc:
                raise exc
            if data:
                data = list(data.values())[0]
                log_obj.log(data)
