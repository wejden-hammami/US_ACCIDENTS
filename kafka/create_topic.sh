#!/usr/bin/env bash
# Cree le topic Kafka et le dossier HDFS de la partie Kafka.
# A lancer depuis la racine du projet, stack demarree (namenode, datanode, zookeeper, kafka).
# Sous Windows : Git Bash (ou lancer les 3 commandes docker exec a la main dans PowerShell).

docker exec kafka kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists \
  --topic us_accidents --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --describe --topic us_accidents

# -u hadoop : le dossier doit appartenir a l'utilisateur HDFS qui ecrira dedans (le consumer)
docker exec -u hadoop namenode hdfs dfs -mkdir -p /data/us_accidents/raw/kafka
docker exec -u hadoop namenode hdfs dfs -ls /data/us_accidents/raw
