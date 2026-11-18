# Pour le Membre 4 (Hive) : tout ce qu'il faut sur les données Kafka

## En bref
- **Dossier HDFS** : `/data/us_accidents/raw/kafka/`
- **Contenu** : 20 fichiers `part-<id>-<n>.csv` de ~6 Mo, soit **1 000 000 de lignes** (les 1 000 000 premières lignes de `US_Accidents_March23.csv`)
- **Format** : CSV, séparateur `,`, guillemets `"` quand un champ contient une virgule, **sans ligne d'en-tête**, UTF-8
- **Valeur manquante** : champ vide (`,,`). Pas de `NA`, pas de `NULL` écrit
- **Table à créer** : `accidents_kafka` (script prêt : `hive_table.sql`)

## Les 15 colonnes, dans l'ordre (Hive lit par position)

| # | Nom commun (= Sqoop, minuscules) | Colonne CSV d'origine | Type conseillé | Exemple | Remarque |
|---|---|---|---|---|---|
| 1 | `id` | ID | STRING | A-14 | Clé d'identification, sert à dédoublonner entre les 3 tables |
| 2 | `severity` | Severity | INT | 2 | De 1 (faible impact) à 4 |
| 3 | `start_time` | Start_Time | TIMESTAMP | 2016-02-08 08:37:07 | Heure locale |
| 4 | `end_time` | End_Time | TIMESTAMP | 2016-02-08 09:07:07 | |
| 5 | `start_lat` | Start_Lat | DOUBLE | 39.79076 | |
| 6 | `start_lng` | Start_Lng | DOUBLE | -84.241547 | |
| 7 | `street` | Street | STRING | Salem Ave | Peut contenir des virgules (champ entre guillemets) |
| 8 | `city` | City | STRING | Dayton | |
| 9 | `state` | State | STRING | OH | |
| 10 | `weather_condition` | Weather_Condition | STRING | Mostly Cloudy | Avec **espaces** (côté Flume : underscores) |
| 11 | `temperature_f` | Temperature(F) | DOUBLE | 36.0 | Fahrenheit |
| 12 | `visibility_mi` | Visibility(mi) | DOUBLE | 10.0 | Miles |
| 13 | `junction` | Junction | TINYINT | 0 | **0 ou 1** (pas true/false) |
| 14 | `traffic_signal` | Traffic_Signal | TINYINT | 1 | **0 ou 1** |
| 15 | `sunrise_sunset` | Sunrise_Sunset | STRING | Day | Day ou Night |

Exemple de ligne réelle :
```
A-14,2,2016-02-08 08:37:07,2016-02-08 09:07:07,39.79076,-84.241547,Salem Ave,Dayton,OH,Mostly Cloudy,36.0,10.0,0,1,Day
```

## Pièges à connaître
1. **OpenCSVSerde lit tout en STRING**, même si on déclare INT ou DOUBLE. D'où la vue `accidents_kafka_typed` (conversion et champ vide → NULL) dans `hive_table.sql`.
2. **Pas de `ROW FORMAT DELIMITED FIELDS TERMINATED BY ','`** : ça casserait les lignes dont `street` contient une virgule.
3. **L'ordre des lignes n'est pas celui du CSV** : Kafka a 3 partitions, l'ordre n'est garanti qu'à l'intérieur d'une partition. Joindre sur `id`, jamais sur la position.
4. **Doublons entre les 3 chemins** (Sqoop, Kafka, Flume) : compter avec `COUNT(DISTINCT id)` ou choisir une source de référence. Un même `id` apparaît une seule fois dans la table Kafka, sauf si le consumer a été rejoué après un arrêt brutal (au plus un lot de 50 000 lignes).
5. **Les noms originaux avec parenthèses** (`Temperature(F)`, `Visibility(mi)`) n'apparaissent nulle part dans les fichiers : pas d'en-tête.
6. **Valeur manquante** : vide côté Kafka, à harmoniser avec Flume (`NA`) avant de comparer les trois tables. Dans la vue typée, `NULLIF(col, '')` règle le cas Kafka.
7. Si `weather_condition` doit être comparée avec la table Flume, remplacer les `_` par des espaces (ou l'inverse) : `REPLACE(weather_condition, '_', ' ')`.

## Créer la table
```powershell
# Copier le script dans le conteneur Hive puis l'exécuter avec beeline
docker cp kafka/hive_table.sql hiveserver2:/tmp/hive_table.sql
docker exec -it hiveserver2 beeline -u jdbc:hive2://localhost:10000 -f /tmp/hive_table.sql
```
Si `beeline` ne répond pas, attendre 1 à 2 minutes que HiveServer2 démarre, ou vérifier `docker logs hiveserver2 --tail 30`.

## Contrôles attendus
| Requête | Résultat attendu |
|---|---|
| `SELECT COUNT(*) FROM accidents_kafka;` | 1000000 |
| `SELECT COUNT(DISTINCT id) FROM accidents_kafka_typed;` | 1000000 (sinon : doublons) |
| `SELECT severity, COUNT(*) ... GROUP BY severity;` | des valeurs de 1 à 4 |

## Si les données HDFS ont disparu
Le NameNode du compose commun reformate HDFS quand il est recréé. Les messages restent dans Kafka (7 jours par défaut) : relancer simplement le consumer (voir `README.md`, section « Reconstruire HDFS »).

## Contact
Partie Kafka : Membre 1 (branche `feature/kafka`).
