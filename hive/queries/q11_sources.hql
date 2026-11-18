SELECT 'sqoop' AS source, COUNT(*) AS lignes, COUNT(DISTINCT id) AS ids_uniques FROM accidents_sqoop_typed
UNION ALL SELECT 'kafka', COUNT(*), COUNT(DISTINCT id) FROM accidents_kafka_typed
UNION ALL SELECT 'flume', COUNT(*), COUNT(DISTINCT id) FROM accidents_flume_typed;
