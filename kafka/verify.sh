#!/usr/bin/env bash
# Verifie que Kafka a bien alimente HDFS. Resultat attendu : 1000000 partout, lag 0.

echo "== Messages dans le topic (somme des 3 partitions) =="
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server kafka:9092 --topic us_accidents

echo "== Retard du consumer (LAG doit etre 0) =="
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 \
  --describe --group hdfs-writer

echo "== Fichiers dans HDFS (dossiers, fichiers, octets) =="
docker exec namenode hdfs dfs -count /data/us_accidents/raw/kafka
docker exec namenode hdfs dfs -ls -h /data/us_accidents/raw/kafka

echo "== Lignes dans HDFS (attendu : 1000000) =="
docker exec namenode bash -c "hdfs dfs -cat /data/us_accidents/raw/kafka/*.csv | wc -l"

echo "== 3 premieres lignes =="
docker exec namenode bash -c "hdfs dfs -cat /data/us_accidents/raw/kafka/*.csv | head -3"
