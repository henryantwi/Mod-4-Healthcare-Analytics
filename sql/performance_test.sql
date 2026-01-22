-- ============================================================
-- Performance Testing Script
-- Comparative Analysis: OLTP vs Star Schema Query Performance
-- ============================================================

-- ============================================================
-- SETUP
-- ============================================================
USE healthcare_db;
SET profiling = 1;


-- ============================================================
-- SECTION 1: OLTP QUERIES
-- ============================================================

SELECT '========================================' as '';
SELECT 'RUNNING OLTP QUERIES' as '';
SELECT '========================================' as '';
SELECT '' as '';

-- ------------------------------------------------------------
-- OLTP Query 1: Monthly Encounters by Specialty
-- ------------------------------------------------------------
SELECT 'OLTP Query 1: Monthly Encounters by Specialty' as 'Running...';

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

SELECT '' as '';

-- ------------------------------------------------------------
-- OLTP Query 2: Top Diagnosis-Procedure Pairs
-- ------------------------------------------------------------
SELECT 'OLTP Query 2: Top Diagnosis-Procedure Pairs' as 'Running...';

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

SELECT '' as '';

-- ------------------------------------------------------------
-- OLTP Query 3: 30-Day Readmission Rate
-- ------------------------------------------------------------
SELECT 'OLTP Query 3: 30-Day Readmission Rate by Specialty' as 'Running...';

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

SELECT '' as '';

-- ------------------------------------------------------------
-- OLTP Query 4: Revenue by Specialty & Month
-- ------------------------------------------------------------
SELECT 'OLTP Query 4: Revenue by Specialty & Month' as 'Running...';

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

SELECT '' as '';
SELECT '========================================' as '';
SELECT 'OLTP QUERIES COMPLETE' as '';
SELECT '========================================' as '';
SELECT '' as '';
SELECT '' as '';


-- ============================================================
-- SECTION 2: STAR SCHEMA QUERIES
-- ============================================================

SELECT '========================================' as '';
SELECT 'RUNNING STAR SCHEMA QUERIES' as '';
SELECT '========================================' as '';
SELECT '' as '';

-- ------------------------------------------------------------
-- Star Query 1: Monthly Encounters by Specialty
-- ------------------------------------------------------------
SELECT 'STAR Query 1: Monthly Encounters by Specialty' as 'Running...';

EXPLAIN ANALYZE
SELECT 
    d.year,
    d.month,
    d.month_name,
    p.specialty_name,
    et.encounter_type_name,
    COUNT(*) AS total_encounters,
    COUNT(DISTINCT f.patient_key) AS unique_patients
FROM fact_encounters f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_provider p ON f.provider_key = p.provider_key
JOIN dim_encounter_type et ON f.encounter_type_key = et.encounter_type_key
GROUP BY 
    d.year,
    d.month,
    d.month_name,
    p.specialty_name,
    et.encounter_type_name
ORDER BY d.year, d.month, p.specialty_name, et.encounter_type_name;

SELECT '' as '';

-- ------------------------------------------------------------
-- Star Query 2: Top Diagnosis-Procedure Pairs
-- ------------------------------------------------------------
SELECT 'STAR Query 2: Top Diagnosis-Procedure Pairs' as 'Running...';

EXPLAIN ANALYZE
SELECT 
    diag.icd10_code,
    diag.icd10_description,
    proc.cpt_code,
    proc.cpt_description,
    COUNT(DISTINCT f.encounter_key) AS encounter_count
FROM fact_encounters f
JOIN bridge_encounter_diagnosis bed ON f.encounter_key = bed.encounter_key
JOIN dim_diagnosis diag ON bed.diagnosis_key = diag.diagnosis_key
JOIN bridge_encounter_procedure bep ON f.encounter_key = bep.encounter_key
JOIN dim_procedure proc ON bep.procedure_key = proc.procedure_key
GROUP BY 
    diag.icd10_code,
    diag.icd10_description,
    proc.cpt_code,
    proc.cpt_description
ORDER BY encounter_count DESC
LIMIT 10;

SELECT '' as '';

-- ------------------------------------------------------------
-- Star Query 3: 30-Day Readmission Rate
-- ------------------------------------------------------------
SELECT 'STAR Query 3: 30-Day Readmission Rate by Specialty' as 'Running...';

EXPLAIN ANALYZE
SELECT 
    p.specialty_name,
    COUNT(DISTINCT f1.encounter_key) AS total_inpatient_encounters,
    COUNT(DISTINCT f2.encounter_key) AS readmissions,
    ROUND(
        COUNT(DISTINCT f2.encounter_key) * 100.0 / 
        NULLIF(COUNT(DISTINCT f1.encounter_key), 0), 
        2
    ) AS readmission_rate_percent
FROM fact_encounters f1
JOIN dim_provider p ON f1.provider_key = p.provider_key
JOIN dim_encounter_type et1 ON f1.encounter_type_key = et1.encounter_type_key
LEFT JOIN fact_encounters f2 ON f1.patient_key = f2.patient_key
    AND f2.encounter_type_key = (SELECT encounter_type_key FROM dim_encounter_type WHERE encounter_type_name = 'Inpatient')
    AND f2.date_key > f1.discharge_date_key
    AND f2.date_key <= f1.discharge_date_key + 30
    AND f2.encounter_key != f1.encounter_key
WHERE et1.encounter_type_name = 'Inpatient'
GROUP BY p.specialty_name
ORDER BY readmission_rate_percent DESC;

SELECT '' as '';

-- ------------------------------------------------------------
-- Star Query 4: Revenue by Specialty & Month
-- ------------------------------------------------------------
SELECT 'STAR Query 4: Revenue by Specialty & Month' as 'Running...';

EXPLAIN ANALYZE
SELECT 
    d.year,
    d.month,
    d.month_name,
    p.specialty_name,
    SUM(f.total_allowed_amount) AS total_revenue,
    COUNT(*) AS encounter_count,
    ROUND(AVG(f.total_allowed_amount), 2) AS avg_revenue_per_encounter
FROM fact_encounters f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_provider p ON f.provider_key = p.provider_key
WHERE f.total_allowed_amount > 0
GROUP BY 
    d.year,
    d.month,
    d.month_name,
    p.specialty_name
ORDER BY d.year, d.month, total_revenue DESC;

SELECT '' as '';
SELECT '========================================' as '';
SELECT 'STAR SCHEMA QUERIES COMPLETE' as '';
SELECT '========================================' as '';


-- ============================================================
-- SECTION 3: SHOW ALL PROFILES
-- ============================================================
SELECT '' as '';
SELECT '' as '';
SELECT '========================================' as '';
SELECT 'PERFORMANCE SUMMARY' as '';
SELECT '========================================' as '';

SHOW PROFILES;

SELECT '' as '';
SELECT 'Copy the execution times from SHOW PROFILES above' as 'Instructions';
SELECT 'Update your query_analysis.txt and star_schema_queries.txt with actual times' as '';


-- ============================================================
-- SECTION 4: PERFORMANCE COMPARISON TABLE
-- ============================================================
SELECT '' as '';
SELECT '========================================' as '';
SELECT 'Fill in this table with times from SHOW PROFILES:' as '';
SELECT '========================================' as '';
SELECT '' as '';
SELECT '| Query | OLTP Time | Star Time | Speedup |' as '';
SELECT '|-------|-----------|-----------|---------|' as '';
SELECT '| Q1    | _____s    | _____s    | ___x    |' as '';
SELECT '| Q2    | _____s    | _____s    | ___x    |' as '';
SELECT '| Q3    | _____s    | _____s    | ___x    |' as '';
SELECT '| Q4    | _____s    | _____s    | ___x    |' as '';


-- ============================================================
-- SCRIPT USAGE
-- ============================================================
-- 
-- 1. Prerequisites:
--    - Verify both OLTP and Star Schema are populated (see setup scripts in src/)
--
-- 2. Execution:
--    - Execute via MySQL client: mysql -u root -p healthcare_db < sql/performance_test.sql
--
-- 3. Output Analysis:
--    - EXPLAIN ANALYZE output provides detailed execution plans and timing
--    - SHOW PROFILES provides summary duration for all executed queries
--
-- 4. Documentation:
--    - Results should be recorded in query_analysis.txt and reflection.md
--
-- ============================================================
