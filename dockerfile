FROM ubuntu:24.04
RUN apt-get update && \
    apt-get install -y software-properties-common && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y python3 python3-pip python3-venv python3-dev && \
    apt-get clean;

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:${JAVA_HOME}/bin:${PATH}"

WORKDIR /app
COPY . /app

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python3", "/app/main.py"]