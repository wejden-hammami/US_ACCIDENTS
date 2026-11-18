SELECT severity, COUNT(*) AS accidents
FROM accidents_ref WHERE severity IS NOT NULL GROUP BY severity ORDER BY severity;
