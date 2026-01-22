-- ============================================================
-- Data Quality Validation Queries
-- Queries to verify data integrity post-ETL load
-- ============================================================

-- ============================================================
-- 1. RECORD COUNT VERIFICATION
-- ============================================================
SELECT 'DIMENSION TABLES' as category, '=' as sep;

SELECT 'dim_date' as table_name, COUNT(*) as record_count FROM dim_date
UNION ALL
SELECT 'dim_patient', COUNT(*) FROM dim_patient
UNION ALL
SELECT 'dim_provider', COUNT(*) FROM dim_provider
UNION ALL
SELECT 'dim_department', COUNT(*) FROM dim_department
UNION ALL
SELECT 'dim_encounter_type', COUNT(*) FROM dim_encounter_type
UNION ALL
SELECT 'dim_diagnosis', COUNT(*) FROM dim_diagnosis
UNION ALL
SELECT 'dim_procedure', COUNT(*) FROM dim_procedure;

SELECT '' as category, '=' as sep;
SELECT 'FACT AND BRIDGE TABLES' as category, '=' as sep;

SELECT 'fact_encounters' as table_name, COUNT(*) as record_count FROM fact_encounters
UNION ALL
SELECT 'bridge_encounter_diagnosis', COUNT(*) FROM bridge_encounter_diagnosis
UNION ALL
SELECT 'bridge_encounter_procedure', COUNT(*) FROM bridge_encounter_procedure;


-- ============================================================
-- 2. REFERENTIAL INTEGRITY CHECKS
-- ============================================================

-- Check for orphaned encounters (should return 0)
SELECT 'Orphaned Patients in Fact' as check_name,
       COUNT(*) as violation_count
FROM fact_encounters f
LEFT JOIN dim_patient p ON f.patient_key = p.patient_key
WHERE p.patient_key IS NULL;

-- Check for orphaned providers (should return 0)
SELECT 'Orphaned Providers in Fact' as check_name,
       COUNT(*) as violation_count
FROM fact_encounters f
LEFT JOIN dim_provider p ON f.provider_key = p.provider_key
WHERE p.provider_key IS NULL;

-- Check for orphaned date keys (should return 0)
SELECT 'Orphaned Date Keys in Fact' as check_name,
       COUNT(*) as violation_count
FROM fact_encounters f
LEFT JOIN dim_date d ON f.date_key = d.date_key
WHERE d.date_key IS NULL;


-- ============================================================
-- 3. DATA QUALITY CHECKS
-- ============================================================

-- Check for NULL values in critical columns
SELECT 'NULL Patient Keys' as check_name,
       COUNT(*) as violation_count
FROM fact_encounters
WHERE patient_key IS NULL;

SELECT 'NULL Provider Keys' as check_name,
       COUNT(*) as violation_count
FROM fact_encounters
WHERE provider_key IS NULL;

-- Check for negative amounts (should be 0)
SELECT 'Negative Claim Amounts' as check_name,
       COUNT(*) as violation_count
FROM fact_encounters
WHERE total_claim_amount < 0;

-- Check for encounters with discharge before admission
SELECT 'Invalid Date Ranges' as check_name,
       COUNT(*) as violation_count
FROM fact_encounters
WHERE discharge_date < encounter_date;


-- ============================================================
-- 4. PRE-AGGREGATION VALIDATION
-- ============================================================

-- Verify diagnosis counts match (OLTP vs Star Schema)
SELECT 'Diagnosis Count Validation' as check_name,
       CASE 
           WHEN oltp_total = star_total THEN 'PASS'
           ELSE 'FAIL'
       END as status,
       oltp_total,
       star_total
FROM (
    SELECT 
        (SELECT SUM(diagnosis_count) FROM fact_encounters) as star_total,
        (SELECT COUNT(*) FROM encounter_diagnoses) as oltp_total
) validation;

-- Verify procedure counts match
SELECT 'Procedure Count Validation' as check_name,
       CASE 
           WHEN oltp_total = star_total THEN 'PASS'
           ELSE 'FAIL'
       END as status,
       oltp_total,
       star_total
FROM (
    SELECT 
        (SELECT SUM(procedure_count) FROM fact_encounters) as star_total,
        (SELECT COUNT(*) FROM encounter_procedures) as oltp_total
) validation;


-- ============================================================
-- 5. BRIDGE TABLE VALIDATION
-- ============================================================

-- Check bridge counts match fact table aggregates
SELECT 
    'Bridge Diagnosis Count' as check_name,
    COUNT(*) as bridge_count,
    (SELECT SUM(diagnosis_count) FROM fact_encounters) as fact_sum,
    CASE 
        WHEN COUNT(*) = (SELECT SUM(diagnosis_count) FROM fact_encounters) THEN 'PASS'
        ELSE 'FAIL'
    END as status
FROM bridge_encounter_diagnosis;

SELECT 
    'Bridge Procedure Count' as check_name,
    COUNT(*) as bridge_count,
    (SELECT SUM(procedure_count) FROM fact_encounters) as fact_sum,
    CASE 
        WHEN COUNT(*) = (SELECT SUM(procedure_count) FROM fact_encounters) THEN 'PASS'
        ELSE 'FAIL'
    END as status
FROM bridge_encounter_procedure;


-- ============================================================
-- 6. SAMPLE DATA VERIFICATION
-- ============================================================

-- Show sample encounter with all dimensions joined
SELECT 
    f.encounter_id,
    p.full_name as patient_name,
    pr.full_name as provider_name,
    pr.specialty_name,
    d.department_name,
    et.encounter_type_name,
    f.encounter_date,
    f.diagnosis_count,
    f.procedure_count,
    f.total_allowed_amount
FROM fact_encounters f
JOIN dim_patient p ON f.patient_key = p.patient_key
JOIN dim_provider pr ON f.provider_key = pr.provider_key
JOIN dim_department d ON f.department_key = d.department_key
JOIN dim_encounter_type et ON f.encounter_type_key = et.encounter_type_key
LIMIT 10;


-- ============================================================
-- 7. PERFORMANCE CHECK: Compare OLTP vs Star Schema
-- ============================================================

-- Enable profiling
SET profiling = 1;

-- Run Query 1 on Star Schema (should be fast)
SELECT 
    d.year,
    d.month,
    p.specialty_name,
    COUNT(*) AS total_encounters
FROM fact_encounters f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_provider p ON f.provider_key = p.provider_key
GROUP BY d.year, d.month, p.specialty_name
LIMIT 10;

-- Show execution time
SHOW PROFILES;
