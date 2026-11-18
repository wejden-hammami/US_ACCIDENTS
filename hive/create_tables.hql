-- =====================================================================
-- Hive : 3 tables externes (une par chemin d'ingestion) + vues typees
-- Rejouable sans risque (DROP ... IF EXISTS). A relancer si le conteneur
-- hive-metastore est recree (le metastore Derby est dans le conteneur).
-- Les 3 chemins ont les memes 15 colonnes logiques :
-- id, severity, start_time, end_time, start_lat, start_lng, street, city,
-- state, weather_condition, temperature_f, visibility_mi, junction,
-- traffic_signal, sunrise_sunset
-- =====================================================================
CREATE DATABASE IF NOT EXISTS accidents;
USE accidents;

-- ---------- KAFKA : CSV avec guillemets, sans en-tete ----------
DROP VIEW  IF EXISTS accidents_kafka_typed;
DROP TABLE IF EXISTS accidents_kafka;
CREATE EXTERNAL TABLE accidents_kafka (
  id STRING, severity STRING, start_time STRING, end_time STRING,
  start_lat STRING, start_lng STRING, street STRING, city STRING, state STRING,
  weather_condition STRING, temperature_f STRING, visibility_mi STRING,
  junction STRING, traffic_signal STRING, sunrise_sunset STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = "\"")
STORED AS TEXTFILE
LOCATION '/data/us_accidents/raw/kafka/';

-- ---------- SQOOP : texte separe par ',' (sans guillemets), null = \N ----------
DROP VIEW  IF EXISTS accidents_sqoop_typed;
DROP TABLE IF EXISTS accidents_sqoop;
CREATE EXTERNAL TABLE accidents_sqoop (
  id STRING, severity STRING, start_time STRING, end_time STRING,
  start_lat STRING, start_lng STRING, street STRING, city STRING, state STRING,
  weather_condition STRING, temperature_f STRING, visibility_mi STRING,
  junction STRING, traffic_signal STRING, sunrise_sunset STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/us_accidents/raw/sqoop/';

-- ---------- FLUME : 1 ligne de log = 1 colonne, parsee dans la vue ----------
-- Format : [Start_Time] ACCIDENT id=..|severity=..|end_time=..|...|sunrise_sunset=..
DROP VIEW  IF EXISTS accidents_flume_typed;
DROP TABLE IF EXISTS accidents_flume_raw;
CREATE EXTERNAL TABLE accidents_flume_raw (line STRING)
STORED AS TEXTFILE
LOCATION '/data/us_accidents/raw/flume/';

-- ---------- Vues typees : meme schema pour les 3 sources ----------
-- NULLIF(x,'') : champ vide -> NULL ; SUBSTR(...,1,19) : coupe ".0" ajoute par Sqoop
-- junction / traffic_signal : accepte 1/0 et true/false (Sqoop ecrit parfois true/false)
CREATE VIEW accidents_kafka_typed AS
SELECT
  id,
  CAST(NULLIF(severity,'') AS INT) AS severity,
  CAST(SUBSTR(NULLIF(start_time,''),1,19) AS TIMESTAMP) AS start_time,
  CAST(SUBSTR(NULLIF(end_time,''),1,19)   AS TIMESTAMP) AS end_time,
  CAST(NULLIF(start_lat,'') AS DOUBLE) AS start_lat,
  CAST(NULLIF(start_lng,'') AS DOUBLE) AS start_lng,
  NULLIF(street,'') AS street, NULLIF(city,'') AS city, NULLIF(state,'') AS state,
  NULLIF(weather_condition,'') AS weather_condition,
  CAST(NULLIF(temperature_f,'') AS DOUBLE) AS temperature_f,
  CAST(NULLIF(visibility_mi,'') AS DOUBLE) AS visibility_mi,
  CASE WHEN LOWER(junction) IN ('1','true') THEN 1 WHEN LOWER(junction) IN ('0','false') THEN 0 END AS junction,
  CASE WHEN LOWER(traffic_signal) IN ('1','true') THEN 1 WHEN LOWER(traffic_signal) IN ('0','false') THEN 0 END AS traffic_signal,
  NULLIF(sunrise_sunset,'') AS sunrise_sunset
FROM accidents_kafka;

CREATE VIEW accidents_sqoop_typed AS
SELECT
  id,
  CAST(NULLIF(severity,'') AS INT) AS severity,
  CAST(SUBSTR(NULLIF(start_time,''),1,19) AS TIMESTAMP) AS start_time,
  CAST(SUBSTR(NULLIF(end_time,''),1,19)   AS TIMESTAMP) AS end_time,
  CAST(NULLIF(start_lat,'') AS DOUBLE) AS start_lat,
  CAST(NULLIF(start_lng,'') AS DOUBLE) AS start_lng,
  NULLIF(street,'') AS street, NULLIF(city,'') AS city, NULLIF(state,'') AS state,
  NULLIF(weather_condition,'') AS weather_condition,
  CAST(NULLIF(temperature_f,'') AS DOUBLE) AS temperature_f,
  CAST(NULLIF(visibility_mi,'') AS DOUBLE) AS visibility_mi,
  CASE WHEN LOWER(junction) IN ('1','true') THEN 1 WHEN LOWER(junction) IN ('0','false') THEN 0 END AS junction,
  CASE WHEN LOWER(traffic_signal) IN ('1','true') THEN 1 WHEN LOWER(traffic_signal) IN ('0','false') THEN 0 END AS traffic_signal,
  NULLIF(sunrise_sunset,'') AS sunrise_sunset
FROM accidents_sqoop;

CREATE VIEW accidents_flume_typed AS
SELECT
  m['id'] AS id,
  CAST(NULLIF(m['severity'],'') AS INT) AS severity,
  CAST(SUBSTR(NULLIF(st,''),1,19) AS TIMESTAMP) AS start_time,
  CAST(SUBSTR(NULLIF(m['end_time'],''),1,19) AS TIMESTAMP) AS end_time,
  CAST(NULLIF(m['start_lat'],'') AS DOUBLE) AS start_lat,
  CAST(NULLIF(m['start_lng'],'') AS DOUBLE) AS start_lng,
  NULLIF(m['street'],'') AS street, NULLIF(m['city'],'') AS city, NULLIF(m['state'],'') AS state,
  NULLIF(REPLACE(m['weather_condition'],'_',' '),'') AS weather_condition,
  CAST(NULLIF(m['temperature_f'],'') AS DOUBLE) AS temperature_f,
  CAST(NULLIF(m['visibility_mi'],'') AS DOUBLE) AS visibility_mi,
  CASE WHEN LOWER(m['junction']) IN ('1','true') THEN 1 WHEN LOWER(m['junction']) IN ('0','false') THEN 0 END AS junction,
  CASE WHEN LOWER(m['traffic_signal']) IN ('1','true') THEN 1 WHEN LOWER(m['traffic_signal']) IN ('0','false') THEN 0 END AS traffic_signal,
  NULLIF(m['sunrise_sunset'],'') AS sunrise_sunset
FROM (
  SELECT regexp_extract(line, '^\\[([^\\]]*)\\]', 1) AS st,
         str_to_map(regexp_extract(line, 'ACCIDENT (.*)$', 1), '[|]', '=') AS m
  FROM accidents_flume_raw
  WHERE line LIKE '%ACCIDENT %'
) t;

-- ---------- Source de reference pour les analyses (a valider par le groupe) ----------
-- Les 3 chemins contiennent les memes accidents : on n'analyse qu'UNE source
-- pour ne pas compter 3 fois. Pour changer de source, remplacer le nom ci-dessous.
DROP VIEW IF EXISTS accidents_ref;
CREATE VIEW accidents_ref AS SELECT * FROM accidents_kafka_typed;

SHOW TABLES;
