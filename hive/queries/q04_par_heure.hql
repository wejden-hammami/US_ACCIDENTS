SELECT HOUR(start_time) AS heure, COUNT(*) AS accidents
FROM accidents_ref WHERE start_time IS NOT NULL GROUP BY HOUR(start_time) ORDER BY heure;
