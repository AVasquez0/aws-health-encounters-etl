-- Row count checks: confirm the Glue job didn't drop or duplicate encounters

-- Enriched table: total rows vs. unique encounter IDs (should match)
SELECT COUNT(*) AS total_rows,
       COUNT(DISTINCT encounter_id) AS unique_encounters
FROM processed_encounters_enriched;

-- Partitioned table: total rows (should match enriched total)
SELECT COUNT(*) AS total_rows
FROM processed_encounters_partitioned;
