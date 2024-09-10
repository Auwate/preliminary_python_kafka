"""
A configuration file that holds important data for the kafka-python-ng interface
"""

import os


ssl_cafile: str = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "secrets/ca.pem"
)
ssl_certfile: str = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "secrets/client-cert.pem"
)
ssl_keyfile: str = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "secrets/client-key.pem"
)
_filepath = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "secrets/client-password"
)
ssl_password: str = os.read(
    os.open(_filepath, os.O_RDONLY), os.stat(_filepath).st_size
).decode()
