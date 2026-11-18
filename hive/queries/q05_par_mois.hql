SELECT DATE_FORMAT(start_time,'yyyy-MM') AS mois, COUNT(*) AS accidents
FROM accidents_ref WHERE start_time IS NOT NULL GROUP BY DATE_FORMAT(start_time,'yyyy-MM') ORDER BY mois;
