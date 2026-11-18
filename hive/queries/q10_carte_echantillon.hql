SELECT start_lat, start_lng, severity, DATE_FORMAT(start_time,'yyyy-MM') AS mois
FROM accidents_ref
WHERE start_lat IS NOT NULL AND start_lng IS NOT NULL AND start_time IS NOT NULL
ORDER BY RAND()
LIMIT 5000;
