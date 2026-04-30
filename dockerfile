FROM ubuntu:24.04
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y openjdk-11-jdk software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update && \
    apt-get install -y python3.11 python3.11-venv python3.11-dev && \
    apt-get clean;

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /app
COPY . /app

RUN python3.11 -m venv ./.venv
ENV PATH="./.venv/bin:$PATH"

# RUN python3.11 -m pip install --upgrade pip --break-system-packages \
#     && python3.11 -m pip install --no-cache-dir -r requirements.txt --break-system-packages

ENTRYPOINT ["./.venv/bin/python3.11", "main.py"]