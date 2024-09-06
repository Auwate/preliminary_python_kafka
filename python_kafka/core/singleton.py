"""
Used to implement the Singleton design
"""


class Singleton:
    """
    Singleton class
    """

    _instance = None

    def __new__(cls, *_, **__):
        if not cls._instance:
            cls._instance = super(Singleton, cls).__new__(cls)
        return cls._instance
