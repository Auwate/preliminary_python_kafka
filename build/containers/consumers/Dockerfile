FROM python:3.12-slim-bookworm

RUN apt update && apt upgrade -y
RUN apt install git -y

WORKDIR /home/program

COPY ./build/containers/consumers/consumer.sh .

RUN chmod +x consumer.sh

RUN python3 -m pip install poetry

WORKDIR /home/program/preliminary_python_kafka

RUN python3 -m poetry install --no-root

CMD [ "./consumer.sh" ]