-- Null checks on key patient fields after the join/enrichment step

SELECT COUNT(*) AS total,
       SUM(CASE WHEN patient_id IS NULL THEN 1 ELSE 0 END) AS null_patient_id,
       SUM(CASE WHEN birth_year IS NULL THEN 1 ELSE 0 END) AS null_birth_year,
       SUM(CASE WHEN sex IS NULL THEN 1 ELSE 0 END) AS null_sex,
       SUM(CASE WHEN state IS NULL THEN 1 ELSE 0 END) AS null_state,
       SUM(CASE WHEN insurance_type IS NULL THEN 1 ELSE 0 END) AS null_insurance_type
FROM processed_encounters_enriched;
