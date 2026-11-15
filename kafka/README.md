# Partie Kafka : ingestion des accidents vers HDFS

Chemin d'ingestion « flux de messages » du projet Big Data US Accidents (Membre 1).

```
CSV (3 Go) → producer.py → topic Kafka "us_accidents" (3 partitions) → consumer.py → HDFS /data/us_accidents/raw/kafka/
```

## Résultats
| Mesure | Valeur |
|---|---|
| Messages envoyés | 1 000 000, 0 erreur, 175 s (≈ 5 700 msg/s) |
| Répartition des partitions | 333 311 / 333 662 / 333 027 |
| Fichiers dans HDFS | 20 CSV de ~6,1 Mo (≈ 122 Mo) |
| Lignes dans HDFS | 1 000 000 |
| Retard du consumer (lag) | 0 |

## Fichiers de ce dossier
| Fichier | Rôle |
|---|---|
| `producer.py` | Lit le CSV, garde 15 colonnes sur 46, nettoie, publie en JSON dans le topic |
| `consumer.py` | Lit le topic, écrit un CSV par lot de 50 000 messages dans HDFS (WebHDFS) |
| `Dockerfile`, `requirements.txt` | Image Python des deux scripts (`confluent-kafka`, `hdfs`) |
| `create_topic.sh` | Crée le topic et le dossier HDFS |
| `verify.sh` | Vérifie les offsets, le lag et le nombre de lignes dans HDFS |
| `hive_table.sql` | Table Hive `accidents_kafka` et vue typée |
| `HIVE.md` | Fiche pour le Membre 4 : format, colonnes, pièges |
| `README.md` | Ce fichier |

Les services `producer` et `consumer` sont déclarés dans le `docker-compose.yml` commun (profil `kafka-app` : ils ne démarrent qu'à la demande).

## Lancer (PowerShell, depuis la racine du projet)
Prérequis : Docker Desktop démarré, et `data/US_Accidents_March23.csv` (non versionné, 3 Go).

```powershell
# 1. Infrastructure
docker compose up -d namenode datanode zookeeper kafka
docker exec -u hadoop namenode hdfs dfsadmin -report | Select-String "Live datanodes"   # Live datanodes (1)

# 2. Topic et dossier HDFS (ou : bash kafka/create_topic.sh)
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic us_accidents --partitions 3 --replication-factor 1
docker exec -u hadoop namenode hdfs dfs -mkdir -p /data/us_accidents/raw/kafka

# 3. Construire les images
docker compose build producer consumer

# 4. Consumer (fenêtre 1), puis producer (fenêtre 2)
docker compose run --rm --no-deps consumer
docker compose run --rm --no-deps producer

# 5. Vérifier (ou : bash kafka/verify.sh)
docker exec namenode bash -c "hdfs dfs -cat /data/us_accidents/raw/kafka/*.csv | wc -l"       # 1000000
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group hdfs-writer   # LAG 0
```

Test rapide (1 000 lignes, sans toucher aux vraies données) :
```powershell
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --create --topic test_accidents --partitions 3 --replication-factor 1
docker compose run --rm --no-deps -e KAFKA_TOPIC=test_accidents -e HDFS_DIR=/data/us_accidents/test_kafka -e IDLE_TIMEOUT=20 -e BATCH_SIZE=500 consumer
docker compose run --rm --no-deps -e KAFKA_TOPIC=test_accidents -e LIMIT=1000 producer
docker exec namenode bash -c "hdfs dfs -cat /data/us_accidents/test_kafka/*.csv | wc -l"   # 1000
```
Simuler un flux : ajouter `-e DELAY=0.05` au producer.

## Reconstruire HDFS (si le NameNode a été recréé)
Kafka garde les messages (7 jours) : pas besoin de relancer le producer.
```powershell
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server kafka:9092 --topic us_accidents   # somme = 1000000
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 --delete --group hdfs-writer
docker compose run --rm --no-deps consumer
```
Si le DataNode ne démarre pas (`Incompatible clusterIDs`) : `docker compose rm -f datanode`, `docker volume rm us_accidents_datanode_data`, `docker compose up -d --no-deps datanode`.

## Format de sortie
CSV, séparateur `,`, guillemets si nécessaire, sans en-tête, valeur manquante = champ vide, `junction` et `traffic_signal` en 0/1, dates `YYYY-MM-DD HH:MM:SS`.
Colonnes dans l'ordre : `id, severity, start_time, end_time, start_lat, start_lng, street, city, state, weather_condition, temperature_f, visibility_mi, junction, traffic_signal, sunrise_sunset`.
Détails et pièges pour Hive : voir `HIVE.md`.

## Choix techniques
- **JSON dans Kafka, CSV dans HDFS** : le JSON est auto-descriptif, le CSV se lit facilement avec Hive.
- **Clé du message = id** : un même accident va toujours dans la même partition ; répartition équilibrée.
- **3 partitions, 1 broker, réplication 1** : montre le parallélisme sans multiplier les conteneurs.
- **`acks=all`, `linger.ms=50`, compression `lz4`** : fiabilité, regroupement des envois, moins de réseau.
- **Commit de l'offset après l'écriture HDFS** (`enable.auto.commit=False`) : *at-least-once*, pas de perte ; au pire un lot en double après un plantage.
- **Lots de 50 000 lignes** : ~20 fichiers de 6 Mo (un fichier par message saturerait HDFS ; un seul fichier saturerait la mémoire).
- **WebHDFS** (port 9870) plutôt que le client Hadoop : le conteneur Python reste léger.
- **Noms communs** (minuscules, comme la table MySQL de Sqoop) : `temperature_f`, `visibility_mi`.

## Limites à reconnaître
- Flux **simulé** : le producer relit un fichier historique, ce n'est pas du temps réel.
- Un seul broker et un seul DataNode : la tolérance aux pannes n'est pas réelle.
- Ordre global perdu entre partitions : joindre sur `id`.
- Les 1 000 000 premières lignes ne sont pas forcément représentatives de toute la période ni de tous les États.
- Pas d'authentification (listener PLAINTEXT, `InsecureClient`) : acceptable pour un TP.
- Pas de garantie *exactly-once*.

## Problèmes rencontrés (utiles à raconter)
| Problème | Cause | Solution |
|---|---|---|
| `dockerDesktopLinuxEngine` introuvable | Docker Desktop non démarré | Lancer l'application |
| NameNode et DataNode qui plantaient | Volumes créés en root, NameNode non formaté | Compose corrigé par l'équipe (entrypoint, chown) |
| HDFS vidé après recréation du NameNode | Formatage automatique au démarrage | Rejouer le consumer : les messages sont toujours dans Kafka |
| DataNode refusait de démarrer | Cluster ID différent après reformatage | Supprimer le volume du DataNode |
| `403` au push | Compte non collaborateur | Accès ajouté par le propriétaire du dépôt |

## Questions de soutenance
- **Pourquoi Kafka, Sqoop et Flume ?** Trois modes d'ingestion : flux de messages, import batch depuis une base, collecte de logs.
- **Kafka écrit-il dans HDFS ?** Non. Le consumer lit le topic et écrit dans HDFS.
- **Topic, partition, offset ?** Topic = canal nommé ; partition = sous-journal ordonné qui permet le parallélisme ; offset = numéro d'un message dans sa partition.
- **Comment Kafka choisit la partition ?** Hash de la clé (ici l'id).
- **À quoi sert le consumer group ?** Partager les partitions entre consumers et mémoriser la progression (`hdfs-writer`).
- **Qu'est-ce que le lag ?** Messages écrits dans Kafka mais pas encore consommés ; ici 0.
- **Si le consumer plante ?** Il reprend au dernier offset validé ; au plus un lot en double.
- **Comment prouver que rien n'est perdu ?** Offsets = 1 000 000, lignes HDFS = 1 000 000, lag = 0.
- **Pourquoi Kafka plutôt qu'une file classique ?** Il conserve les messages après lecture : on peut rejouer (c'est ce qui a permis de reconstruire HDFS).
- **Pourquoi ZooKeeper ?** Métadonnées du cluster Kafka (version 7.4.0) ; remplacé par KRaft dans les versions récentes.
- **Quelle différence entre Kafka et Flume ?** Kafka conserve et redistribue les messages à plusieurs lecteurs ; Flume transporte des événements (surtout des logs) vers une destination via Source, Channel, Sink.
- **Comment le rendre temps réel ?** Un vrai producteur en continu ; ici le délai entre messages simule le flux.
- **Comment passer à l'échelle ?** Plus de partitions et de consumers, plusieurs brokers (réplication 3), plusieurs DataNodes.
