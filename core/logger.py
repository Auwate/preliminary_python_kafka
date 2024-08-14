"""
Description: A logger class that implements a singleton design.
"""

from .singleton import Singleton


class Logger(Singleton):
    """
    Logger class
    """

    def __init__(self, log_file: str) -> None:
        try:
            self.log_file = open(log_file, "+a")
        except Exception as exc:
            raise ValueError from exc

    def log(self, values: list[str]) -> bool:
        for value in values:
            print(value, file=self.log_file)
        return True
