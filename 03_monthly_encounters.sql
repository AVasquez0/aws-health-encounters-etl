-- Encounter volume by month

SELECT date_trunc('month', encounter_date) AS month,
       COUNT(*) AS encounters
FROM processed_encounters_enriched
GROUP BY 1
ORDER BY 1;
