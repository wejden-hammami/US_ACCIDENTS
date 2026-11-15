-- Table Hive pour les donnees ingerees par Kafka (partie Membre 1 -> Membre 4).
-- Fichiers : CSV, separateur ',', guillemets '"', SANS en-tete, UTF-8, valeur manquante = champ vide.
-- OpenCSVSerde gere les virgules a l'interieur de street (champs entre guillemets).
-- Attention : OpenCSVSerde lit TOUT en STRING, d'ou la vue typee plus bas.

CREATE DATABASE IF NOT EXISTS accidents;
USE accidents;

DROP TABLE IF EXISTS accidents_kafka;
CREATE EXTERNAL TABLE accidents_kafka (
  id                STRING,
  severity          STRING,
  start_time        STRING,
  end_time          STRING,
  start_lat         STRING,
  start_lng         STRING,
  street            STRING,
  city              STRING,
  state             STRING,
  weather_condition STRING,
  temperature_f     STRING,
  visibility_mi     STRING,
  junction          STRING,
  traffic_signal    STRING,
  sunrise_sunset    STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = "\"")
STORED AS TEXTFILE
LOCATION '/data/us_accidents/raw/kafka/';

-- Vue typee : champ vide -> NULL, puis conversion des types.
DROP VIEW IF EXISTS accidents_kafka_typed;
CREATE VIEW accidents_kafka_typed AS
SELECT
  id,
  CAST(NULLIF(severity, '')       AS INT)       AS severity,
  CAST(NULLIF(start_time, '')     AS TIMESTAMP) AS start_time,
  CAST(NULLIF(end_time, '')       AS TIMESTAMP) AS end_time,
  CAST(NULLIF(start_lat, '')      AS DOUBLE)    AS start_lat,
  CAST(NULLIF(start_lng, '')      AS DOUBLE)    AS start_lng,
  NULLIF(street, '')                            AS street,
  NULLIF(city, '')                              AS city,
  NULLIF(state, '')                             AS state,
  NULLIF(weather_condition, '')                 AS weather_condition,
  CAST(NULLIF(temperature_f, '')  AS DOUBLE)    AS temperature_f,
  CAST(NULLIF(visibility_mi, '')  AS DOUBLE)    AS visibility_mi,
  CAST(NULLIF(junction, '')       AS TINYINT)   AS junction,
  CAST(NULLIF(traffic_signal, '') AS TINYINT)   AS traffic_signal,
  NULLIF(sunrise_sunset, '')                    AS sunrise_sunset
FROM accidents_kafka;

-- Controles
SELECT COUNT(*) FROM accidents_kafka;                          -- attendu : 1000000
SELECT COUNT(DISTINCT id) FROM accidents_kafka_typed;          -- doublons si different du total
SELECT severity, COUNT(*) FROM accidents_kafka_typed GROUP BY severity ORDER BY severity;
SELECT state, COUNT(*) AS nb FROM accidents_kafka_typed GROUP BY state ORDER BY nb DESC LIMIT 10;
