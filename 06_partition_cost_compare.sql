-- Partition pruning test: same question (March 2026 encounters + avg billed),
-- run against the unpartitioned and partitioned tables.
--
-- Results (Athena engine v3, 2026-10-21):
--   unpartitioned: 26.28 KB scanned
--   partitioned:    0.98 KB scanned  (~27x less data)

-- Confirm partitions are registered
SHOW PARTITIONS processed_encounters_partitioned;

-- Unpartitioned: full scan, filter on the date column
SELECT COUNT(*) AS encounters, ROUND(AVG(billed_amount), 2) AS avg_billed
FROM processed_encounters_enriched
WHERE encounter_date BETWEEN DATE '2026-03-01' AND DATE '2026-03-31';

-- Partitioned: filter on partition keys so Athena only reads year=2026/month=3
SELECT COUNT(*) AS encounters, ROUND(AVG(billed_amount), 2) AS avg_billed
FROM processed_encounters_partitioned
WHERE year = '2026' AND month = '3';
