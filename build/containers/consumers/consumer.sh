#!/bin/bash

# Get dependencies
python3 -m pip install poetry
python3 -m poetry install --no-root

# Clone the repo
git clone https://github.com/Auwate/preliminary_python_kafka.git -b testing
cd preliminary_python_kafka/

# Run consumer application
python3 -m consumer_module