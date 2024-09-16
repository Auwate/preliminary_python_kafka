FROM debian:bookworm-slim

WORKDIR /home/program

COPY volume.sh .
RUN chmod +x volume.sh

CMD [ "./volume.sh" ]