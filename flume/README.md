# Flume - collecte des logs d'accidents (Membre 3)

Chaine : US_Accidents_March23.csv -> generate_logs.py -> flume/logs/accidents.log -> Flume -> HDFS

## Fichiers
- generate_logs.py : lit le CSV ligne par ligne et ecrit une ligne de log par accident
- flume.conf : agent Flume (Source TAILDIR, Channel Memory, Sink HDFS)
- Dockerfile : image Flume 1.11.0 + bibliotheques Hadoop 3.3.6 (copiees depuis apache/hadoop:3.3.6)

## Format du log
[Start_Time] ACCIDENT id=...|severity=...|end_time=...|start_lat=...|start_lng=...|street=...|city=...|state=...|weather_condition=...|temperature_f=...|visibility_mi=...|junction=...|traffic_signal=...|sunrise_sunset=...
Valeur manquante = vide ; Junction et Traffic_Signal = 1/0 ; dates sur 19 caracteres.
Les lignes sont les 1 000 000 premieres lignes du CSV, comme pour Kafka et Sqoop.

## Lancer
1. docker compose up -d namenode datanode
2. docker compose exec -u hadoop namenode hdfs dfs -mkdir -p /data/us_accidents/raw/flume
3. Fenetre A : docker compose run --rm --no-deps -e HADOOP_USER_NAME=hadoop flume
4. Fenetre B : python flume/generate_logs.py --csv ./US_Accidents_March23.csv --limit 1000000
5. Attendre la fermeture des fichiers (aucun .tmp dans HDFS) puis verifier : hdfs dfs -cat accidents.* | wc -l

## Resultat valide
1 000 000 de lignes et 1 000 000 d'identifiants uniques dans /data/us_accidents/raw/flume (5 fichiers, environ 283 Mo).

## Limites
- Les logs sont generes a partir d'un dataset historique : ce ne sont pas de vrais logs de cameras.
- Channel Memory : des evenements non transferes peuvent etre perdus en cas d'arret brutal.
- Le fichier de position de TAILDIR est dans le conteneur : un nouveau conteneur relit accidents.log depuis le debut. Avant de relancer, vider accidents.log et le dossier HDFS.
