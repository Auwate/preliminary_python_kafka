#!/bin/bash

REPO_NAME="https://github.com/Auwate/preliminary_python_kafka.git -b testing"
DIR_NAME="preliminary_python_kafka"

# Clone the repo

if [ -d "$DIR_NAME" ]; then
    rm -rf "$DIR_NAME"
fi

git clone "$REPO_NAME"
cd "$DIR_NAME"

# Get dependencies
python3 -m pip install poetry
python3 -m poetry install --no-root

# Run consumer application
python3 -m poetry run python3 -m consumer_module