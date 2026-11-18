SELECT ordre, classe, COUNT(*) AS accidents, ROUND(AVG(severity),2) AS gravite_moyenne
FROM (
  SELECT severity,
    CASE WHEN temperature_f < 32 THEN 1 WHEN temperature_f < 50 THEN 2 WHEN temperature_f < 70 THEN 3 WHEN temperature_f < 90 THEN 4 ELSE 5 END AS ordre,
    CASE WHEN temperature_f < 32 THEN '< 32 F' WHEN temperature_f < 50 THEN '32 - 50 F' WHEN temperature_f < 70 THEN '50 - 70 F' WHEN temperature_f < 90 THEN '70 - 90 F' ELSE '90 F et +' END AS classe
  FROM accidents_ref WHERE temperature_f IS NOT NULL
) t GROUP BY ordre, classe ORDER BY ordre;
