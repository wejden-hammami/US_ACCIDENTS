# Lance TOUTE la chaine : Kafka + Flume + Sqoop -> HDFS -> Hive -> Dashboard
# Usage (PowerShell, depuis la racine du projet) :
#   .\run_all.ps1                 # test rapide : 20 000 lignes par chemin
#   .\run_all.ps1 -Limit 1000000  # volume complet (long)
# Prerequis : Docker Desktop demarre, python + pip, data\US_Accidents_March23.csv
param([int]$Limit = 20000, [switch]$SkipKafka, [switch]$SkipFlume, [switch]$SkipSqoop)

$csv = "data/US_Accidents_March23.csv"
$hs2 = "jdbc:hive2://localhost:10000"
function Step($m) { Write-Host "`n=== $m ===" -ForegroundColor Cyan }
if (-not (Test-Path $csv)) { Write-Host "CSV manquant : $csv" -ForegroundColor Red; exit 1 }

Step "1/6 Infrastructure (HDFS, YARN, Kafka, MySQL, Hive)"
docker compose up -d namenode datanode resourcemanager nodemanager zookeeper kafka mysql sqoop hive-metastore hiveserver2
for ($i = 0; $i -lt 36; $i++) {
  $ok = docker exec -u hadoop namenode hdfs dfsadmin -report 2>$null | Select-String "Live datanodes \(1\)"
  if ($ok) { break }; Start-Sleep 5
}
docker exec -u hadoop namenode hdfs dfs -mkdir -p /data/us_accidents/raw/kafka /data/us_accidents/raw/flume /data/us_accidents/raw/sqoop
docker exec -u hadoop namenode hdfs dfs -chmod -R 777 /data

if (-not $SkipKafka) {
  Step "2/6 Kafka -> HDFS ($Limit lignes)"
  Start-Sleep 20
  docker exec kafka kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic us_accidents --partitions 3 --replication-factor 1
  docker compose build producer consumer
  $cid = (docker compose run -d --no-deps consumer) | Select-Object -Last 1
  docker compose run --rm --no-deps -e LIMIT=$Limit producer
  docker wait $cid | Out-Null     # le consumer s'arrete apres 60 s sans message
}

if (-not $SkipFlume) {
  Step "3/6 Flume -> HDFS ($Limit lignes)"
  python flume/generate_logs.py --csv $csv --limit $Limit --force
  docker compose up -d flume
  for ($i = 0; $i -lt 60; $i++) {          # attendre que les fichiers .tmp soient fermes
    Start-Sleep 10
    $ls = docker exec -u hadoop namenode hdfs dfs -ls /data/us_accidents/raw/flume 2>$null
    if (($ls -match "accidents") -and -not ($ls -match "\.tmp")) { break }
  }
}

if (-not $SkipSqoop) {
  Step "4/6 MySQL -> Sqoop -> HDFS ($Limit lignes)"
  for ($i = 0; $i -lt 40; $i++) {
    docker exec mysql mysql -uroot -proot -e "SELECT COUNT(*) FROM accidents_db.accidents" 2>$null | Out-Null
    if ($LASTEXITCODE -eq 0) { break }; Start-Sleep 5
  }
  pip install -q pandas mysql-connector-python
  $env:CSV_PATH = (Resolve-Path $csv).Path
  $env:NROWS = "$Limit"
  python sqoop/load_data.py
  docker cp sqoop/import.sh sqoop:/tmp/import.sh
  docker exec -e HADOOP_USER_NAME=hadoop sqoop bash -c "tr -d '\r' < /tmp/import.sh | bash"
}

Step "5/6 Hive : tables + controles"
for ($i = 0; $i -lt 40; $i++) {
  docker exec hiveserver2 beeline -u $hs2 -e "SHOW DATABASES;" 2>$null | Out-Null
  if ($LASTEXITCODE -eq 0) { break }; Start-Sleep 10
}
docker exec hiveserver2 beeline -u $hs2 -f /opt/scripts/create_tables.hql
docker exec hiveserver2 beeline -u $hs2 -f /opt/scripts/check.hql

Step "6/6 Dashboard"
docker exec hiveserver2 bash -c "tr -d '\r' < /opt/scripts/export_all.sh | bash"
docker compose up -d --build dashboard
Write-Host "`nTermine -> http://localhost:8050   (interface HDFS : http://localhost:9870)" -ForegroundColor Green
