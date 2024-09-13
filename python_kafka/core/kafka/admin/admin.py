"""
The Admin class for managing Kafka topics using KafkaAdminClient.
"""

from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import IncompatibleBrokerVersion
from ....configs.configs import ssl_cafile, ssl_certfile, ssl_keyfile, ssl_password


class Admin:
    """
    A class for managing Kafka topics using the KafkaAdminClient.

    This class provides methods for creating Kafka topics and retrieving topic details.

    Attributes:
    -----------
    _admin : KafkaAdminClient
        An instance of KafkaAdminClient used for interacting with Kafka.

    Methods:
    --------
    admin() -> KafkaAdminClient:
        Returns the KafkaAdminClient instance.

    get_topic_details(topic_name: str) -> None | dict:
        Retrieves the details of a Kafka topic, such as partitions, leader, and replicas.
        Returns None if the topic is not found.

    create_topic(
        topic_name: str,
        num_partitions: int,
        repli_factor: int,
        timeout_ms: int | None
    ) -> tuple[bool, Exception]:
        Creates a Kafka topic with the specified number of partitions and replication factor.
        Returns a tuple where the first element is a boolean indicating success, and the second
        element is an exception if any occurred.
    """

    def __init__(
        self,
        bs_servers: str,
        sec_protocol: str,
        check_hostname: bool,
    ):
        """
        Initializes the Admin class with KafkaAdminClient.

        Parameters:
        -----------
        bs_servers : str
            The Kafka bootstrap servers.
        sec_protocol : str
            The security protocol for connecting to Kafka (e.g., "SSL", "PLAINTEXT").
        check_hostname : bool
            Whether to check the hostname in SSL certificates.
        """
        self._admin = KafkaAdminClient(
            bootstrap_servers=bs_servers,
            security_protocol=sec_protocol,
            ssl_check_hostname=check_hostname,
            ssl_cafile=ssl_cafile,
            ssl_certfile=ssl_certfile,
            ssl_keyfile=ssl_keyfile,
            ssl_password=ssl_password,
        )

    @property
    def admin(self) -> KafkaAdminClient:
        """
        Returns the KafkaAdminClient instance used to manage Kafka topics.

        Returns:
        --------
        KafkaAdminClient:
            The KafkaAdminClient instance.
        """
        return self._admin

    def get_topic_details(
        self, topic_name: str
    ) -> None | dict[str, int | str | list[dict[str, int | list[int]]]]:
        """
        Retrieves the details of a Kafka topic.

        Parameters:
        -----------
        topic_name : str
            The name of the Kafka topic to retrieve details for.

        Returns:
        --------
        None | dict:
            A dictionary containing details about the Kafka topic
            (e.g., partitions, leader, replicas). Returns None if the topic is not found.
        """
        topic = self.admin.describe_topics([topic_name])
        return topic[0] if topic[0]["partitions"] else None

    def create_topic(
        self,
        topic_name: str,
        num_partitions: int,
        repli_factor: int,
        timeout_ms: int | None,
    ) -> tuple[bool, Exception]:
        """
        Creates a Kafka topic with the specified parameters.

        Parameters:
        -----------
        topic_name : str
            The name of the topic to be created.
        num_partitions : int
            The number of partitions for the topic.
        repli_factor : int
            The replication factor for the topic.
        timeout_ms : int | None
            The timeout in milliseconds for the topic creation operation (optional).

        Returns:
        --------
        tuple[bool, Exception]:
            A tuple where the first element is a boolean indicating whether the topic was
            successfully created, and the second element is an exception if any error occurred.
        """
        try:
            topic = NewTopic(
                name=topic_name,
                num_partitions=num_partitions,
                replication_factor=repli_factor,
            )
            self.admin.create_topics([topic], timeout_ms=timeout_ms)
            description: dict[str, object] = self.get_topic_details(
                topic_name=topic_name
            )
            assert description and description["topic"] == topic_name
        except IncompatibleBrokerVersion as exc:
            return False, exc
        except AssertionError as exc:
            return False, exc
        return True, None
