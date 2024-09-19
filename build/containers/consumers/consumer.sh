#!/bin/bash
REPO_NAME="https://github.com/Auwate/preliminary_python_kafka.git"
DIR_NAME="preliminary_python_kafka"

# Clone the repo
git clone "$REPO_NAME" -b fixes
cd "$DIR_NAME"

# Copy the volume into the application
mkdir ./secrets
cp -r ../secrets_volume/* ./secrets

# Get dependencies
python3 -m pip install poetry --quiet --root-user-action=ignore
python3 -m poetry install --no-root --quiet

# Run consumer application
exec python3 -m poetry run python3 -m consumer_module