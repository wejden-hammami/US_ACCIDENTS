SELECT ordre, classe, COUNT(*) AS accidents, ROUND(AVG(severity),2) AS gravite_moyenne
FROM (
  SELECT severity,
    CASE WHEN visibility_mi < 1 THEN 1 WHEN visibility_mi < 5 THEN 2 WHEN visibility_mi < 10 THEN 3 ELSE 4 END AS ordre,
    CASE WHEN visibility_mi < 1 THEN '< 1 mi' WHEN visibility_mi < 5 THEN '1 - 5 mi' WHEN visibility_mi < 10 THEN '5 - 10 mi' ELSE '10 mi et +' END AS classe
  FROM accidents_ref WHERE visibility_mi IS NOT NULL
) t GROUP BY ordre, classe ORDER BY ordre;
