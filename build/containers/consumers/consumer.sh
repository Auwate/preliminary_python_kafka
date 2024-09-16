#!/bin/bash
REPO_NAME="https://github.com/Auwate/preliminary_python_kafka.git"
DIR_NAME="preliminary_python_kafka"

# Clone the repo
git clone "$REPO_NAME" -b testing
cd "$DIR_NAME"

mkdir ./secrets
cp -r ../secrets_volume/* ./secrets

# Get dependencies
python3 -m pip install poetry
python3 -m poetry install --no-root

# Run consumer application
python3 -m poetry run python3 -m consumer_module