SELECT city, state, COUNT(*) AS accidents
FROM accidents_ref WHERE city IS NOT NULL
GROUP BY city, state ORDER BY accidents DESC LIMIT 15;
