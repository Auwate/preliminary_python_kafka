
import asyncio
from .consumer import Consumer


class ConsumerBuilder:

    def __init__(self):
        self._topic = ""
        self._group = ""
        self._bootstrap_servers = "localhost:9092"
        self._security_protocol = "SSL"
        self._ssl_check_hostname = False

    def bootstrap_servers(self, bootstrap_servers: str) -> "ConsumerBuilder":
        if not bootstrap_servers:
            return self
        if not isinstance(bootstrap_servers, str):
            raise ValueError("Bootstrap servers is not of type str.")
        self._bootstrap_servers = bootstrap_servers
        return self

    def security_protocol(self, security_protocol: str) -> "ConsumerBuilder":
        if not security_protocol:
            return self
        if not isinstance(security_protocol, str):
            raise ValueError("Security protocol is not of type str.")
        self._security_protocol = security_protocol
        return self

    def ssl_check_hostname(self, ssl_check_hostname: bool) -> "ConsumerBuilder":
        if not ssl_check_hostname:
            return self
        if not isinstance(ssl_check_hostname, bool):
            raise ValueError("SSL check hostname is not of type bool.")
        self._ssl_check_hostname = ssl_check_hostname
        return self

    def topic(self, topic: str) -> "ConsumerBuilder":
        if not topic:
            return self
        if not isinstance(topic, str):
            raise ValueError("Topic is not of type str.")
        self._topic = topic
        return self

    def group(self, group: str) -> "ConsumerBuilder":
        if group and not isinstance(group, str):
            raise ValueError("Group is not of type str.")
        self._group = group
        return self

    def build(self) -> "Consumer":
        return Consumer(
            bs_servers=self._bootstrap_servers,
            sec_protocol=self._security_protocol,
            ssl_check_hostname=self._ssl_check_hostname,
            topic=self._topic,
            group=self._group,
        )
