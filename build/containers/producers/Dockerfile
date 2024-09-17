# Slim-bookworm was chosen for the Debian interface with a minimal size
FROM python:3.12-slim-bookworm

# Update dependencies
RUN apt update && apt upgrade -y
RUN apt install git -y

# Organize the application's install
WORKDIR /home/program

# Copy the install script
COPY producer.sh .
RUN chmod +x producer.sh

# Run the install script
CMD [ "./producer.sh" ]