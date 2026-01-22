-- ============================================================
-- Healthcare Analytics: OLTP Performance Analysis Queries
-- Queries designed to identify potential performance bottlenecks
-- ============================================================

-- Enable profiling to measure query execution time
SET profiling = 1;

-- Execution metrics are obtained using EXPLAIN ANALYZE
    

-- ============================================================
-- QUERY 1: Monthly Encounters by Specialty
-- ============================================================
-- Purpose: For each month and specialty, show total encounters 
--          and unique patients by encounter type.
-- Tables Joined: encounters, providers, specialties (2 JOINs)
-- ============================================================

-- ============================================================
-- QUERY 1: Monthly Encounters by Specialty
-- ============================================================
-- Purpose: For each month and specialty, show total encounters 
--          and unique patients by encounter type.
-- Tables Joined: encounters, providers, specialties (2 JOINs)
-- ============================================================

-- Query execution plan:
EXPLAIN ANALYZE
SELECT 
    DATE_FORMAT(e.encounter_date, '%Y-%m') AS month,
    s.specialty_name,
    e.encounter_type,
    COUNT(*) AS total_encounters,
    COUNT(DISTINCT e.patient_id) AS unique_patients
FROM encounters e
JOIN providers p ON e.provider_id = p.provider_id
JOIN specialties s ON p.specialty_id = s.specialty_id
GROUP BY 
    DATE_FORMAT(e.encounter_date, '%Y-%m'),
    s.specialty_name,
    e.encounter_type
ORDER BY month, s.specialty_name, e.encounter_type;


-- ============================================================
-- QUERY 2: Top Diagnosis-Procedure Pairs
-- ============================================================
-- Purpose: Find the most common diagnosis-procedure combinations.
-- Tables Joined: encounters, encounter_diagnoses, diagnoses, 
--                encounter_procedures, procedures (4 JOINs)
-- ============================================================
-- QUERY 2: Top Diagnosis-Procedure Pairs
-- ============================================================
-- Purpose: Find the most common diagnosis-procedure combinations.
-- Tables Joined: encounters, encounter_diagnoses, diagnoses, 
--                encounter_procedures, procedures (4 JOINs)
-- Warning: High cost due to two junction tables causing row explosion.
-- ============================================================

-- Query execution plan:
EXPLAIN ANALYZE
SELECT 
    d.icd10_code,
    d.icd10_description,
    pr.cpt_code,
    pr.cpt_description,
    COUNT(DISTINCT e.encounter_id) AS encounter_count
FROM encounters e
JOIN encounter_diagnoses ed ON e.encounter_id = ed.encounter_id
JOIN diagnoses d ON ed.diagnosis_id = d.diagnosis_id
JOIN encounter_procedures ep ON e.encounter_id = ep.encounter_id
JOIN procedures pr ON ep.procedure_id = pr.procedure_id
GROUP BY 
    d.icd10_code,
    d.icd10_description,
    pr.cpt_code,
    pr.cpt_description
ORDER BY encounter_count DESC
LIMIT 10;


-- ============================================================
-- QUERY 3: 30-Day Readmission Rate by Specialty
-- ============================================================
-- Purpose: Find which specialty has the highest readmission rate.
-- Definition: Inpatient discharge, then return within 30 days.
-- Tables Joined: encounters (self-join), providers, specialties
-- Warning: Self-join operation is resource intensive on large tables.
-- ============================================================

-- Query execution plan:
EXPLAIN ANALYZE
SELECT 
    s.specialty_name,
    COUNT(DISTINCT e1.encounter_id) AS total_inpatient_encounters,
    COUNT(DISTINCT e2.encounter_id) AS readmissions,
    ROUND(
        COUNT(DISTINCT e2.encounter_id) * 100.0 / 
        NULLIF(COUNT(DISTINCT e1.encounter_id), 0), 
        2
    ) AS readmission_rate_percent
FROM encounters e1
JOIN providers p ON e1.provider_id = p.provider_id
JOIN specialties s ON p.specialty_id = s.specialty_id
LEFT JOIN encounters e2 ON e1.patient_id = e2.patient_id
    AND e2.encounter_type = 'Inpatient'
    AND e2.encounter_date > e1.discharge_date
    AND e2.encounter_date <= DATE_ADD(e1.discharge_date, INTERVAL 30 DAY)
    AND e2.encounter_id != e1.encounter_id
WHERE e1.encounter_type = 'Inpatient'
GROUP BY s.specialty_name
ORDER BY readmission_rate_percent DESC;


-- ============================================================
-- QUERY 4: Revenue by Specialty & Month
-- ============================================================
-- Purpose: Total allowed amounts by specialty and month.
-- Tables Joined: billing, encounters, providers, specialties (3 JOINs)
-- ============================================================

-- Query execution plan:
EXPLAIN ANALYZE
SELECT 
    DATE_FORMAT(b.claim_date, '%Y-%m') AS month,
    s.specialty_name,
    SUM(b.allowed_amount) AS total_revenue,
    COUNT(*) AS claim_count,
    ROUND(AVG(b.allowed_amount), 2) AS avg_revenue_per_claim
FROM billing b
JOIN encounters e ON b.encounter_id = e.encounter_id
JOIN providers p ON e.provider_id = p.provider_id
JOIN specialties s ON p.specialty_id = s.specialty_id
GROUP BY 
    DATE_FORMAT(b.claim_date, '%Y-%m'),
    s.specialty_name
ORDER BY month, total_revenue DESC;


-- ============================================================
-- Show all query execution times
-- ======================================================== ===
SHOW PROFILES;

