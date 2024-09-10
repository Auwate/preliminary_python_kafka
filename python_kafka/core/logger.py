"""
Description: A logger class that implements a singleton design.
"""

from .singleton import Singleton


class Logger(Singleton):
    """
    Logger class
    """

    def __init__(self, log_file: str) -> None:
        self.log_file = log_file

    def log(self, values: list[str]) -> bool:
        """
        Explanation:
            A function that takes data from a list and logs it
        Args:
            values: (list[str]) A list of string elements that will be logged.
        """
        with open(self.log_file, "a+", encoding="utf8") as file:
            for value in values:
                print(value, file=file)
        return True
