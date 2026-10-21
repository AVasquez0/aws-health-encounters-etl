-- Billed vs. paid amounts by department

SELECT department,
       COUNT(*) AS encounters,
       ROUND(AVG(billed_amount), 2) AS avg_billed,
       ROUND(AVG(paid_amount), 2) AS avg_paid,
       ROUND(SUM(paid_amount) / SUM(billed_amount) * 100, 1) AS pct_paid
FROM processed_encounters_enriched
GROUP BY department
ORDER BY avg_billed DESC;
