-- 30-day readmission rate by insurance type

-- Check what values readmitted_30d actually holds before filtering on it
SELECT DISTINCT readmitted_30d
FROM processed_encounters_enriched;

SELECT insurance_type,
       COUNT(*) AS encounters,
       SUM(CASE WHEN readmitted_30d = 'Y' THEN 1 ELSE 0 END) AS readmissions,
       ROUND(100.0 * SUM(CASE WHEN readmitted_30d = 'Y' THEN 1 ELSE 0 END) / COUNT(*), 1) AS readmit_rate_pct
FROM processed_encounters_enriched
GROUP BY insurance_type
ORDER BY readmit_rate_pct DESC;
