FROM apache/hadoop:3.3.6 AS hadoop

FROM eclipse-temurin:8-jre

ENV FLUME_VERSION=1.11.0
ENV FLUME_TGZ=apache-flume-1.11.0-bin.tar.gz
ENV FLUME_HOME=/opt/flume
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$PATH:$FLUME_HOME/bin:$HADOOP_HOME/bin

RUN apt-get update && apt-get install -y wget && \
    wget -q https://archive.apache.org/dist/flume/${FLUME_VERSION}/${FLUME_TGZ} && \
    tar -xzf ${FLUME_TGZ} && \
    mv apache-flume-${FLUME_VERSION}-bin ${FLUME_HOME} && \
    rm ${FLUME_TGZ} && \
    apt-get remove -y wget && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

COPY --from=hadoop /opt/hadoop /opt/hadoop

RUN mkdir -p /opt/flume/conf /tmp/logs_accidents

WORKDIR /opt/flume
