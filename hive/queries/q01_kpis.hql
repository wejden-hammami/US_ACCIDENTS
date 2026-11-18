SELECT COUNT(*) AS accidents, COUNT(DISTINCT id) AS ids_uniques, COUNT(DISTINCT state) AS etats,
       ROUND(AVG(severity),2) AS gravite_moyenne,
       CAST(MIN(start_time) AS STRING) AS premiere_date, CAST(MAX(start_time) AS STRING) AS derniere_date
FROM accidents_ref;
