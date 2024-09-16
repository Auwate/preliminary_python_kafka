#!/bin/bash

# Clone the repo
git clone https://github.com/Auwate/preliminary_python_kafka.git -b testing
cd preliminary_python_kafka/

# Get dependencies
python3 -m pip install poetry
python3 -m poetry install --no-root

# Run consumer application
python3 -m poetry run python3 -m consumer_module