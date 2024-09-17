"""
Integration test suite for the Producer class
"""

import unittest
from python_kafka.core.kafka.producer.producer_builder import ProducerBuilder
from python_kafka.core.kafka.producer.producer import Producer


class TestProducerBuilder(unittest.TestCase):
    """
    Specific test class for ProducerBuilder
    """

    def setUp(self) -> None:
        """
        unittest setUp function
        """
        self.producer: Producer = ProducerBuilder().topic("TEST").build()

    def test_builder_creates_producer(self):
        """
        Assert that the created value of .build() is "Producer"
        """
        self.assertIsInstance(self.producer, Producer, type(self.producer))
