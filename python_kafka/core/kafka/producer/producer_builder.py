"""
The builder class for Producer
"""

from .producer import Producer


class ProducerBuilder:

    def __init__(self):
        self._topic = "Test"
        self._acks: int | str = 0  # Can be 0, 1 or "all"
        self._bootstrap_servers = "localhost:9092"
        self._security_protocol = "SSL"
        self._ssl_check_hostname = False

    def acks(self, ack: int | str) -> "ProducerBuilder":
        if not ack:
            return self
        if not isinstance(ack, str) and not isinstance(ack, int):
            raise ValueError("Ack is not of type str or int.")
        self._ack = ack
        return self

    def topic(self, topic: str) -> "ProducerBuilder":
        if not topic:
            return self
        if not isinstance(topic, str):
            raise ValueError("Topic is not of type str.")
        self._topic = topic
        return self

    def bootstrap_servers(self, bootstrap_servers: str) -> "ProducerBuilder":
        if not bootstrap_servers:
            return self
        if not isinstance(bootstrap_servers, str):
            raise ValueError("Bootstrap servers is not of type str.")
        self._bootstrap_servers = bootstrap_servers
        return self

    def security_protocol(self, security_protocol: str) -> "ProducerBuilder":
        if not security_protocol:
            return self
        if not isinstance(security_protocol, str):
            raise ValueError("Security protocol is not of type str.")
        self._security_protocol = security_protocol
        return self

    def ssl_check_hostname(self, ssl_check_hostname: bool) -> "ProducerBuilder":
        if not ssl_check_hostname:
            return self
        if not isinstance(ssl_check_hostname, bool):
            raise ValueError("SSL check hostname is not of type bool.")
        self._ssl_check_hostname = ssl_check_hostname
        return self

    def build(self) -> Producer:
        return Producer(
            topic=self._topic,
            acks=self._acks,
            bs_servers=self._bootstrap_servers,
            sec_protocol=self._security_protocol,
            check_hostname=self._ssl_check_hostname,
        )
