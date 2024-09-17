"""
Testing class for ProducerBuilder
"""

import unittest
from python_kafka.core.kafka.producer.producer_builder import ProducerBuilder


class TestSuite(unittest.TestCase):
    """
    Unit tests for the ProducerBuilder class.

    The test suite validates individual and complete configuration updates
    of the ProducerBuilder class, ensuring correct value assignment for
    each attribute of the Producer.
    """

    def test_builder_update_only_topic(self):
        """
        Test: Updates only the topic of the ProducerBuilder.

        Verifies:
        ---------
        - The topic value is correctly set when modified using the `topic()` method.
        """
        producer = ProducerBuilder().topic("Test")
        self.assertEqual(
            producer._topic, "Test", producer._topic  # pylint: disable=W0212
        )

    def test_builder_update_only_bootstrap_servers(self):
        """
        Test: Updates only the bootstrap servers of the ProducerBuilder.

        Verifies:
        ---------
        - The bootstrap_servers value is correctly set when modified using the
        `bootstrap_servers()` method.
        """
        producer = ProducerBuilder().bootstrap_servers("localhost:9091")
        self.assertEqual(
            producer._bootstrap_servers,  # pylint: disable=W0212
            "localhost:9091",
            producer._bootstrap_servers,  # pylint: disable=W0212
        )

    def test_builder_update_only_security_protocol(self):
        """
        Test: Updates only the security protocol of the ProducerBuilder.

        Verifies:
        ---------
        - The security_protocol value is correctly set when modified using the
        `security_protocol()` method.
        """
        producer = ProducerBuilder().security_protocol("Plaintext")
        self.assertEqual(
            producer._security_protocol,  # pylint: disable=W0212
            "Plaintext",
            producer._security_protocol,  # pylint: disable=W0212
        )

    def test_builder_update_only_check_hostname(self):
        """
        Test: Updates only the SSL check_hostname flag of the ProducerBuilder.

        Verifies:
        ---------
        - The ssl_check_hostname value is correctly set when modified using the
        `ssl_check_hostname()` method.
        """
        producer = ProducerBuilder().ssl_check_hostname(True)
        self.assertTrue(
            producer._ssl_check_hostname,  # pylint: disable=W0212
            producer._ssl_check_hostname,  # pylint: disable=W0212
        )

    def test_builder_update_all(self):
        """
        Test: Updates all values in the ProducerBuilder.

        Verifies:
        ---------
        - Correct configuration of topic, bootstrap_servers, security_protocol,
          and ssl_check_hostname when all attributes are set.
        """
        producer = (
            ProducerBuilder()
            .topic("Test2")
            .bootstrap_servers("localhost")
            .security_protocol("Plaintext")
            .ssl_check_hostname(True)
        )

        self.assertEqual(
            producer._topic, "Test2", producer._topic  # pylint: disable=W0212
        )
        self.assertEqual(
            producer._bootstrap_servers,  # pylint: disable=W0212
            "localhost",
            producer._bootstrap_servers,  # pylint: disable=W0212
        )
        self.assertEqual(
            producer._security_protocol,  # pylint: disable=W0212
            "Plaintext",
            producer._security_protocol,  # pylint: disable=W0212
        )
        self.assertTrue(
            producer._ssl_check_hostname,  # pylint: disable=W0212
            producer._ssl_check_hostname,  # pylint: disable=W0212
        )

    def test_builder_default_values(self):
        """
        Test: Checks the default values of the ProducerBuilder.

        Verifies:
        ---------
        - The default values for topic, bootstrap_servers, security_protocol,
          and ssl_check_hostname are correctly assigned in the ProducerBuilder constructor.
        """
        producer = ProducerBuilder()
        self.assertEqual(
            producer._topic, "Test", producer._topic  # pylint: disable=W0212
        )
        self.assertEqual(
            producer._bootstrap_servers,  # pylint: disable=W0212
            "localhost:9092",
            producer._bootstrap_servers,  # pylint: disable=W0212
        )
        self.assertEqual(
            producer._security_protocol,  # pylint: disable=W0212
            "SSL",
            producer._security_protocol,  # pylint: disable=W0212
        )
        self.assertFalse(
            producer._ssl_check_hostname,  # pylint: disable=W0212
            producer._ssl_check_hostname,  # pylint: disable=W0212
        )
