SELECT weather_condition, COUNT(*) AS accidents, ROUND(AVG(severity),2) AS gravite_moyenne
FROM accidents_ref WHERE weather_condition IS NOT NULL
GROUP BY weather_condition ORDER BY accidents DESC LIMIT 15;
