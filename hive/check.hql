-- Controles : a lancer apres create_tables.hql (attendu : 1000000 partout)
USE accidents;
SELECT 'kafka_lignes' AS controle, COUNT(*) AS valeur FROM accidents_kafka
UNION ALL SELECT 'kafka_ids_uniques', COUNT(DISTINCT id) FROM accidents_kafka_typed
UNION ALL SELECT 'sqoop_lignes', COUNT(*) FROM accidents_sqoop
UNION ALL SELECT 'sqoop_ids_uniques', COUNT(DISTINCT id) FROM accidents_sqoop_typed
UNION ALL SELECT 'sqoop_lignes_decalees(street avec virgule?)', COUNT(*) FROM accidents_sqoop WHERE sunrise_sunset IS NOT NULL AND sunrise_sunset NOT IN ('Day','Night')
UNION ALL SELECT 'flume_lignes', COUNT(*) FROM accidents_flume_raw
UNION ALL SELECT 'flume_ids_uniques', COUNT(DISTINCT id) FROM accidents_flume_typed;
