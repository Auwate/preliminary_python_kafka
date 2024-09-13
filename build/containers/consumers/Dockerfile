FROM python:3.12-slim-bookworm

RUN apt update && apt upgrade -y
RUN apt install git -y

WORKDIR /home/program

RUN git clone https://github.com/Auwate/preliminary_python_kafka.git -b testing
RUN python3 -m pip install poetry

WORKDIR /home/program/preliminary_python_kafka

RUN python3 -m poetry install --no-root

CMD [ "python3", "-m", "poetry", "run", "python3", "-m", "consumer_module" ]