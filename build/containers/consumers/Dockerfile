# Slim-bookworm was chosen for the Debian interface with a minimal size
FROM python:3.12-slim-bookworm

# Update dependencies
RUN apt update && apt upgrade -y
RUN apt install git -y

# Organize the application's install
WORKDIR /home/program

# Copy the install script
COPY ./build/containers/consumers/consumer.sh .
RUN chmod +x consumer.sh

# Run the install script
CMD [ "./consumer.sh" ]