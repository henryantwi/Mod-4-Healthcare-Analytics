-- ============================================================
-- Healthcare Analytics: Star Schema Queries (OLAP)
-- Optimized analytical queries using the dimensional model
-- ============================================================

-- Enable profiling to compare with OLTP queries
SET profiling = 1;

-- Execution metrics are obtained using EXPLAIN ANALYZE



-- ============================================================
-- QUERY 1: Monthly Encounters by Specialty (OPTIMIZED)
-- ============================================================
-- OLTP Version: 3 tables, 2 JOINs, DATE_FORMAT function
-- Star Schema: 3 tables, 2 JOINs, pre-joined specialty in dim_provider
-- 
-- ============================================================
-- QUERY 1: Monthly Encounters by Specialty (OPTIMIZED)
-- ============================================================
-- OLTP Version: 3 tables, 2 JOINs, DATE_FORMAT function
-- Star Schema: 3 tables, 2 JOINs, pre-joined specialty in dim_provider
-- 
-- Optimization Details: 
-- - No DATE_FORMAT() function - use date dimension
-- - Specialty already denormalized in dim_provider
-- - Direct access to month/year from dim_date
-- ============================================================

-- Query execution plan:
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

-- Performance Notes:
-- OLTP: encounters → providers → specialties (2 JOINs + DATE_FORMAT)
-- Star: fact_encounters → dim_date + dim_provider (no function calls)
-- Expected improvement: ~3-5x faster due to:
--   - Integer key joins instead of string comparisons
--   - No date parsing function
--   - Specialty pre-denormalized in provider dimension


-- ============================================================
-- QUERY 2: Top Diagnosis-Procedure Pairs (OPTIMIZED)
-- ============================================================
-- OLTP Version: 5 tables, 4 JOINs, two junction tables (row explosion!)
-- Star Schema: 4 tables, 3 JOINs via bridge tables
--
-- ============================================================
-- QUERY 2: Top Diagnosis-Procedure Pairs (OPTIMIZED)
-- ============================================================
-- OLTP Version: 5 tables, 4 JOINs, two junction tables (row explosion!)
-- Star Schema: 4 tables, 3 JOINs via bridge tables
--
-- Optimization Details:
-- - Bridge tables are indexed and optimized
-- - Direct key lookups instead of junction table scans
-- ============================================================

-- Query execution plan:
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

-- Performance Notes:
-- This query still requires bridge table joins (many-to-many)
-- BUT bridge tables have:
--   - Surrogate key indexes (faster than natural key lookups)
--   - Smaller row size (just keys, no redundant data)
-- Expected improvement: ~2x faster


-- ============================================================
-- QUERY 3: 30-Day Readmission Rate by Specialty (OPTIMIZED)
-- ============================================================
-- OLTP Version: Self-join on encounters + providers + specialties
-- Star Schema: Self-join on fact + dim_provider (specialty pre-joined)
--
-- ============================================================
-- QUERY 3: 30-Day Readmission Rate by Specialty (OPTIMIZED)
-- ============================================================
-- OLTP Version: Self-join on encounters + providers + specialties
-- Star Schema: Self-join on fact + dim_provider (specialty pre-joined)
--
-- Optimization Details:
-- - Specialty already in dim_provider (eliminates 1 JOIN)
-- - date_key allows efficient date range comparisons
-- - Pre-calculated length_of_stay available
-- ============================================================

-- Query execution plan:
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
    AND f2.date_key <= f1.discharge_date_key + 30   -- Approximate: 30 days
    AND f2.encounter_key != f1.encounter_key
WHERE et1.encounter_type_name = 'Inpatient'
GROUP BY p.specialty_name
ORDER BY readmission_rate_percent DESC;

-- Alternative using date dimension for precise 30-day calculation:
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
JOIN dim_date d1 ON f1.discharge_date_key = d1.date_key
LEFT JOIN fact_encounters f2 ON f1.patient_key = f2.patient_key
    JOIN dim_date d2 ON f2.date_key = d2.date_key
    AND f2.encounter_type_key = (SELECT encounter_type_key FROM dim_encounter_type WHERE encounter_type_name = 'Inpatient')
    AND d2.calendar_date > d1.calendar_date
    AND d2.calendar_date <= DATE_ADD(d1.calendar_date, INTERVAL 30 DAY)
    AND f2.encounter_key != f1.encounter_key
WHERE et1.encounter_type_name = 'Inpatient'
GROUP BY p.specialty_name
ORDER BY readmission_rate_percent DESC;

-- Performance Notes:
-- Self-join still required (inherent to readmission logic)
-- BUT improvements:
--   - One less JOIN (specialty in provider dimension)
--   - Integer date_key comparisons instead of DATETIME
-- Expected improvement: ~2-3x faster


-- ============================================================
-- QUERY 4: Revenue by Specialty & Month (OPTIMIZED)
-- ============================================================
-- OLTP Version: billing → encounters → providers → specialties (3 JOINs)
-- Star Schema: fact → dim_date + dim_provider (2 JOINs)
--
-- ============================================================
-- QUERY 4: Revenue by Specialty & Month (OPTIMIZED)
-- ============================================================
-- OLTP Version: billing → encounters → providers → specialties (3 JOINs)
-- Star Schema: fact → dim_date + dim_provider (2 JOINs)
--
-- Optimization Details:
-- - Billing data (total_allowed_amount) pre-aggregated in fact
-- - No billing table join required!
-- - Specialty denormalized in dim_provider
-- - No DATE_FORMAT function
-- ============================================================

-- Query execution plan:
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
WHERE f.total_allowed_amount > 0  -- Only encounters with billing
GROUP BY 
    d.year,
    d.month,
    d.month_name,
    p.specialty_name
ORDER BY d.year, d.month, total_revenue DESC;

-- Performance Notes:
-- OLTP: billing → encounters → providers → specialties (3 JOINs + DATE_FORMAT)
-- Star: fact_encounters → dim_date + dim_provider (2 JOINs)
-- Expected improvement: ~5-10x faster due to:
--   - Billing already in fact table (no JOIN)
--   - Specialty in provider dimension (no extra JOIN)
--   - Integer date_key instead of DATE_FORMAT
--   - Pre-aggregated allowed_amount


-- ============================================================
-- Show execution times for all queries
-- ============================================================
SHOW PROFILES;


-- ============================================================
-- PERFORMANCE COMPARISON SUMMARY
-- ============================================================
-- 
-- | Query | OLTP JOINs | Star JOINs | Expected Speedup |
-- |-------|------------|------------|------------------|
-- | Q1    | 2          | 2          | 3-5x             |
-- | Q2    | 4          | 4          | 2x               |
-- | Q3    | 3+self     | 2+self     | 2-3x             |
-- | Q4    | 3          | 2          | 5-10x            |
--
-- KEY PERFORMANCE FACTORS:
-- 1. Pre-aggregated metrics (diagnosis_count, total_allowed_amount)
-- 2. Denormalized dimensions (specialty in provider)
-- 3. Integer surrogate keys (faster joins)
-- 4. Date dimension (no date functions)
-- 5. Optimized indexes on fact and dimension tables
-- ============================================================
