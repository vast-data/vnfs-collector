FROM python:3.12-slim-bookworm

WORKDIR /opt/nfsops

ENV PYTHONUNBUFFERED=1

COPY dist/vnfs-collector_0.0.1_all.deb .
RUN apt update && apt install ./vnfs-collector_0.0.1_all.deb -y

ENTRYPOINT ["vnfs"]
