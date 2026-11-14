FROM openjdk:8-jdk-slim

ENV FLUME_VERSION=1.11.0
ENV FLUME_HOME=/opt/flume
ENV PATH=$PATH:$FLUME_HOME/bin

RUN apt-get update && apt-get install -y wget && \
    wget -q https://archive.apache.org/dist/flume/${FLUME_VERSION}/apache-flume-${FLUME_VERSION}-bin.tar.gz && \
    tar -xzf apache-flume-${FLUME_VERSION}-bin.tar.gz && \
    mv apache-flume-${FLUME_VERSION}-bin ${FLUME_HOME} && \
    rm apache-flume-${FLUME_VERSION}-bin.tar.gz && \
    apt-get remove -y wget && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/flume/conf /tmp/logs_accidents

WORKDIR /opt/flume
