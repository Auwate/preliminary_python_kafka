# What is this?
    # - This is a small, temporary container that copies the SSL dependencies into the new volume

# Could have used Alpine, but bookworm-slim works just as well
FROM debian:bookworm-slim

WORKDIR /home/program

COPY volume.sh .
RUN chmod +x volume.sh

CMD [ "./volume.sh" ]