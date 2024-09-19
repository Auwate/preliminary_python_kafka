# CIARA Kafka Testing w/ `kafka-python-ng`

This is a project that was built during my internship at CIARA. It is a Python CLI tool that provides efficiency benchmarking between a producer container, consumer container, and Kafka cluster.

To get started, please follow the `Prerequisities` header below and then to `Running the application`.

**IMPORTANT**

This application was built on a Unix environment, so directions are going to be Unix-centric. In addition, this application may behave as intended on a Windows host.

# Prerequisities

## Virtual environment

This project uses `Poetry`, so to run this application you will need to install it either locally or as root. We recommend installing it via a `venv`, and you do this using `python3 -m venv venv`.

Once you do that, please type `source venv/bin/activate`.

### Poetry

After creating your virtual environment, please type `pip install poetry`.

Once installed, please `cd` into `preliminary_python_kafka` and type `poetry install --no-root`.

If this doesn't work, please try `python3 -m poetry install --no-root`.

## SSL Dependencies

In `./preliminary_python_kafka/build/ssl_dependencies`, you will find the `build.sh` script. Please run this file and it should build the dependencies and launch Kafka.

## Troubleshooting

If `build.sh` is not working, please try running `chmod 777 build.sh` and then running again.

# Running the application

With your virtual environment set up and the SSL dependencies having been built, we can now start running the application.

The basic syntax is:

```
python3 -m main_module
```

To see all arguments and parameters, please type:

```
python3 -m main_module --help
```

You will see a message like this pop up:

```
usage: __main__.py [-h] -P PRODUCERS -C CONSUMERS [-BS BOOTSTRAP_SERVER] [-SP SECURITY_PROTOCOL] [-SCHN SSL_CHECK_HOSTNAME] [-G GROUP] [-T TOPIC] [-A ACKS]
                   [-W WORKERS]

Configure Kafka producers, consumers, and other settings.

options:
  -h, --help            show this help message and exit
  -P PRODUCERS, --producers PRODUCERS
                        Type: int | Specify the number of producers that will send messages to Kafka.
  -C CONSUMERS, --consumers CONSUMERS
                        Type: int | Specify the number of consumers that will read messages from Kafka.
  -BS BOOTSTRAP_SERVER, --bootstrap-server BOOTSTRAP_SERVER
                        Type: str (Optional) The location of the Kafka cluster (default: localhost:9092).
  -SP SECURITY_PROTOCOL, --security-protocol SECURITY_PROTOCOL
                        Type: str (Optional) The security protocol for Kafka communication (default: SSL).
  -SCHN SSL_CHECK_HOSTNAME, --ssl-check-hostname SSL_CHECK_HOSTNAME
                        Type: bool (Optional) A flag to enable SSL hostname verification (default: False).
  -G GROUP, --group GROUP
                        Type: str (Optional) The Kafka consumer group ID (default: Test Group).
  -T TOPIC, --topic TOPIC
                        Type: str (Optional) The Kafka topic (default: Test Topic).
  -A ACKS, --acks ACKS  Type: str (Optional) The acknowledgement pattern between Kafka and producer.Values can be 0, 1 or 'all'
  -W WORKERS, --workers WORKERS
                        Type: int (Optional) The amount of worker threads that will handle blocking code.
```

# DEV - Setup

## Poetry

This project uses `Poetry` for dependency management and execution. To use this, please create a virtual environment and run `pip install poetry`.

Next, please go to the root of the project and run `poetry install --with dev --no-root`

## Tox

This project uses `Tox` for standardized and automated testing. To use this, please run `poetry run tox`.

If you would like to specify a specific test to run, use `poetry run tox -e <TEST_NAME>`

# Update History

## 09/19/2024

- Added average messages sent/consumed
- Addressed issue regarding acks

## 09/17/2024

MAJOR UPDATE

- Added argument parsing
- Added consumer container
- Added producer container
- Added volumes for SSL encryption data
- Added exception handling
- Added documentation

## 09/15/2024

- Added kafka/producer
- Added kafka/admin

## 09/14/2024

- Added kafka/consumer

## 09/12/2024

- Added build/
    - Added build.sh
- Added kafka/
    - Added producer/
        - Added builder abstraction for producer class
- Added testing_producer_builder