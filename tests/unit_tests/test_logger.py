"""
Explanation:
    A testing class for logger.py
"""

import unittest
import os
from ...core.logger import Logger


class TestLogger(unittest.TestCase):
    """
    Explanation:
        A testing suite for the features of logger.py
    """

    def setUp(self) -> None:
        """
        Explanation:
            Set up the logger and related dependencies
        """
        self.file_name: str = "test.txt"
        self.logger_obj = Logger(self.file_name)

    def tearDown(self) -> None:
        """
        Explanation:
            Remove the test file
        """
        if os.path.exists("./test.txt"):
            os.remove(self.file_name)

    def test_singleton(self) -> None:
        """
        Explanation:
            This test function makes sure the Singleton feature is working
        """
        logger_test = Logger("test")
        self.assertEqual(self.logger_obj, logger_test)
