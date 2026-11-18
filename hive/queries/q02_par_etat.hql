SELECT state, COUNT(*) AS accidents, ROUND(AVG(severity),2) AS gravite_moyenne
FROM accidents_ref WHERE state IS NOT NULL GROUP BY state ORDER BY accidents DESC;
