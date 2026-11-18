# Guide Membre 4 — Hive + Dashboard (pas à pas)

## 0. Où on en est (lu dans vos 4 archives)
- `main` contient déjà : cluster Hadoop, Kafka, MySQL, Sqoop (importé), et **tous les services dans un seul `docker-compose.yml`** (y compris `hive-metastore` et `hiveserver2`, mais incomplets : ils ne savaient pas où est HDFS).
- Il manque dans `main` : la version récente de **`feature/kafka`** (3 commits : README, `HIVE.md`, `hive_table.sql`, consumer/producer à jour) et **`feature/flume`** (`flume.conf`, `generate_logs.py`, Dockerfile Flume corrigé).
- Ta branche `feature/hive-dashboard` est vide (0 en avance) : tout le travail Hive reste à faire.

**« Merger dans le docker compose » = en fait un simple `git merge`** : le compose est déjà commun. Ce que j'ai changé dans `docker-compose.yml` : uniquement les services Hive (+ ajout du service `dashboard`).

## 1. Récupérer le travail de tout le monde

### Option A — avec git (recommandé, PowerShell dans le dossier du repo)
```powershell
git fetch origin
git switch feature/hive-dashboard
git merge origin/main
git merge origin/feature/kafka
git merge origin/feature/flume
```
Si git annonce un **conflit** sur un fichier : pour `kafka/*` ou `flume/*` garde la version de la branche fusionnée (`git checkout --theirs <fichier>` puis `git add <fichier>`) ; pour `docker-compose.yml` garde les deux blocs (celui de `main` avec `producer`/`consumer`). Puis `git commit`.

Ensuite **copie par-dessus** les fichiers de ce zip : `docker-compose.yml`, `hive/`, `dashboard/`, `.gitattributes`, `GUIDE_HIVE.md`.

### Option B — sans git
Dézippe ce zip dans ton dossier de repo en écrasant : il contient déjà `main` + `feature/kafka` + `feature/flume` + la partie Hive/Dashboard. Puis `git add -A`, `git commit -m "feat(hive): tables, requetes, dashboard"`, `git push origin feature/hive-dashboard`.

## 2. Le CSV (non versionné, 3 Go)
Télécharge `US_Accidents_March23.csv` (Kaggle) dans `data/US_Accidents_March23.csv`.

## 3. Mettre des données dans TON HDFS (point important)
Le HDFS est **local à chaque machine** : celui de tes collègues n'est pas chez toi. Et `namenode-entrypoint.sh` **reformate HDFS à chaque démarrage du namenode** → ne redémarre pas le namenode en cours de route, sinon tout est vide.

Commence **par Kafka** (le mieux documenté), tu développeras Hive dessus ; Flume et Sqoop viennent après.

```powershell
docker compose up -d namenode datanode zookeeper kafka
docker exec -u hadoop namenode hdfs dfsadmin -report | Select-String "Live datanodes"      # doit afficher (1)
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic us_accidents --partitions 3 --replication-factor 1
docker exec -u hadoop namenode hdfs dfs -mkdir -p /data/us_accidents/raw/kafka
docker compose build producer consumer
docker compose run --rm --no-deps consumer      # fenêtre 1 (laisser tourner)
docker compose run --rm --no-deps producer      # fenêtre 2
docker exec namenode bash -c "hdfs dfs -cat /data/us_accidents/raw/kafka/*.csv | wc -l"     # 1000000
```
Pour tester vite : `-e LIMIT=20000` sur le producer.

**Flume** (voir `flume/README.md`) : `docker exec -u hadoop namenode hdfs dfs -mkdir -p /data/us_accidents/raw/flume`, puis `docker compose run --rm --no-deps -e HADOOP_USER_NAME=hadoop flume` et, dans une autre fenêtre, `python flume/generate_logs.py --csv ./data/US_Accidents_March23.csv --limit 1000000`.

**Sqoop** : demande à ta collègue Sqoop la séquence exacte (le dossier `sqoop/` n'a pas de README). En gros : `docker compose up -d mysql sqoop resourcemanager nodemanager`, `python sqoop/load_data.py` (modifier `CSV_PATH` vers `data/US_Accidents_March23.csv`), puis lancer `sqoop/import.sh` dans le conteneur `sqoop`.

## 4. Démarrer Hive
Pas besoin de Kafka/MySQL/Flume pour ça, seulement HDFS + Hive :
```powershell
docker compose up -d namenode datanode hive-metastore hiveserver2
docker logs hiveserver2 --tail 20          # attendre 1 à 2 min que le port 10000 soit ouvert
```

## 5. Créer les tables et vérifier
```powershell
docker exec hiveserver2 beeline -u jdbc:hive2://localhost:10000 -f /opt/scripts/create_tables.hql
docker exec hiveserver2 beeline -u jdbc:hive2://localhost:10000 -f /opt/scripts/check.hql
```
`create_tables.hql` crée `accidents_kafka`, `accidents_sqoop`, `accidents_flume_raw` + 3 vues typées (même schéma) + `accidents_ref`. Les tables dont le dossier HDFS est vide renvoient simplement 0 ligne.
À relancer si le conteneur `hive-metastore` est recréé (Derby est dans le conteneur).

Requête libre : `docker exec -it hiveserver2 beeline -u jdbc:hive2://localhost:10000/accidents`

## 6. Dashboard
```powershell
docker exec hiveserver2 bash /opt/scripts/export_all.sh     # 11 requêtes -> dashboard/data/*.csv (quelques minutes)
docker compose up -d --build dashboard                      # http://localhost:8050
```
La page relit les CSV à chaque rafraîchissement. En cas d'échec d'une requête : `dashboard/data/<nom>.err`.
**Aperçu sans Hive** (données *synthétiques*, bandeau orange affiché) : `cd dashboard; pip install dash pandas; $env:DATA_DIR="sample_data"; python app.py`.

## 7. Décisions à valider avec le groupe (cadrage)
- **Source de référence** : `accidents_ref` pointe sur Kafka. Les 3 sources contiennent les mêmes accidents → ne jamais additionner les 3 tables. Changer la source = 1 ligne dans `create_tables.hql`.
- **Sqoop** : l'import écrit du texte *sans guillemets* ; un `street` contenant une virgule décale les colonnes. `check.hql` compte ces lignes (`sqoop_lignes_decalees`). Si > 0, voir avec la collègue Sqoop (`--optionally-enclosed-by`).
- Volume commun : 1 000 000 de lignes (déjà le cas dans les 3 branches).

## 8. Si ça bloque
| Symptôme | Piste |
|---|---|
| `beeline` : connection refused | HiveServer2 pas prêt : attendre, `docker logs hiveserver2`, `docker logs hive-metastore` |
| Permission denied sur HDFS | vérifier `HADOOP_USER_NAME: hadoop` sur les 2 services Hive |
| `Incompatible clusterIDs` (datanode) | voir `kafka/README.md` (supprimer le volume datanode) |
| Table vide | `docker exec namenode hdfs dfs -ls /data/us_accidents/raw/<source>` |
| `.sh` : `\r: command not found` | fins de ligne Windows ; `.gitattributes` fourni, refaire `git add --renormalize .` |

## 9. Tout lancer d'un coup
`.\run_all.ps1` (test 20 000 lignes) ou `.\run_all.ps1 -Limit 1000000` : lance Kafka, Flume et Sqoop vers HDFS, crée les tables Hive, exporte les résultats et démarre le dashboard. Options : `-SkipKafka`, `-SkipFlume`, `-SkipSqoop`. Machine chargée : ~8 Go de RAM libres pour Docker recommandés.
