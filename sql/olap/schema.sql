-- ============================================================
-- Healthcare Analytics Lab - Part 3.2: STAR SCHEMA DDL
-- Optimized dimensional model for healthcare analytics
-- ============================================================

-- ============================================================
-- DROP EXISTING STAR SCHEMA TABLES (if re-running)
-- ============================================================
DROP TABLE IF EXISTS bridge_encounter_procedure;
DROP TABLE IF EXISTS bridge_encounter_diagnosis;
DROP TABLE IF EXISTS fact_encounters;
DROP TABLE IF EXISTS dim_procedure;
DROP TABLE IF EXISTS dim_diagnosis;
DROP TABLE IF EXISTS dim_encounter_type;
DROP TABLE IF EXISTS dim_department;
DROP TABLE IF EXISTS dim_provider;
DROP TABLE IF EXISTS dim_patient;
DROP TABLE IF EXISTS dim_date;


-- ============================================================
-- DIMENSION: dim_date
-- Purpose: Calendar dimension for time-based analysis
-- Grain: One row per calendar date
-- ============================================================
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,              -- Format: YYYYMMDD (e.g., 20240115)
    calendar_date DATE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    week_of_year INT NOT NULL,
    day_of_month INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN DEFAULT FALSE,
    fiscal_year INT,
    fiscal_quarter INT,
    
    INDEX idx_calendar_date (calendar_date),
    INDEX idx_year_month (year, month)
);


-- ============================================================
-- DIMENSION: dim_patient
-- Purpose: Patient demographics with derived attributes
-- Grain: One row per patient (SCD Type 1 - overwrite)
-- ============================================================
CREATE TABLE dim_patient (
    patient_key INT PRIMARY KEY AUTO_INCREMENT,
    patient_id INT NOT NULL,               -- Natural key from OLTP
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(200),                -- Derived: first + last
    date_of_birth DATE,
    gender CHAR(1),
    gender_description VARCHAR(10),        -- Derived: Male/Female
    age INT,                               -- Derived: calculated age
    age_group VARCHAR(20),                 -- Derived: 0-17, 18-34, etc.
    mrn VARCHAR(20),
    
    -- Audit columns
    effective_date DATE DEFAULT (CURRENT_DATE),
    
    UNIQUE INDEX idx_patient_id (patient_id),
    INDEX idx_age_group (age_group),
    INDEX idx_gender (gender)
);


-- ============================================================
-- DIMENSION: dim_provider
-- Purpose: Healthcare providers with denormalized specialty
-- Grain: One row per provider
-- Note: Department info accessed via dim_department (no redundancy)
-- ============================================================
CREATE TABLE dim_provider (
    provider_key INT PRIMARY KEY AUTO_INCREMENT,
    provider_id INT NOT NULL,              -- Natural key from OLTP
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(200),                -- Derived
    credential VARCHAR(20),
    
    -- Denormalized from specialties table (no separate dim_specialty)
    specialty_id INT,
    specialty_name VARCHAR(100),
    specialty_code VARCHAR(10),
    
    UNIQUE INDEX idx_provider_id (provider_id),
    INDEX idx_specialty (specialty_name)
);


-- ============================================================
-- DIMENSION: dim_department
-- Purpose: Hospital departments (kept separate for flexibility)
-- Grain: One row per department
-- ============================================================
CREATE TABLE dim_department (
    department_key INT PRIMARY KEY AUTO_INCREMENT,
    department_id INT NOT NULL,            -- Natural key from OLTP
    department_name VARCHAR(100),
    floor INT,
    capacity INT,
    
    UNIQUE INDEX idx_department_id (department_id)
);


-- ============================================================
-- DIMENSION: dim_encounter_type
-- Purpose: Encounter type lookup (small dimension)
-- Grain: One row per encounter type
-- ============================================================
CREATE TABLE dim_encounter_type (
    encounter_type_key INT PRIMARY KEY AUTO_INCREMENT,
    encounter_type_name VARCHAR(50) NOT NULL,
    encounter_type_category VARCHAR(50),   -- Grouping if needed
    
    UNIQUE INDEX idx_encounter_type_name (encounter_type_name)
);


-- ============================================================
-- DIMENSION: dim_diagnosis
-- Purpose: ICD-10 diagnosis codes
-- Grain: One row per diagnosis code
-- ============================================================
CREATE TABLE dim_diagnosis (
    diagnosis_key INT PRIMARY KEY AUTO_INCREMENT,
    diagnosis_id INT NOT NULL,             -- Natural key from OLTP
    icd10_code VARCHAR(10) NOT NULL,
    icd10_description VARCHAR(200),
    
    -- Derived categorizations
    diagnosis_category VARCHAR(100),       -- Could be derived from code
    
    UNIQUE INDEX idx_diagnosis_id (diagnosis_id),
    INDEX idx_icd10_code (icd10_code)
);


-- ============================================================
-- DIMENSION: dim_procedure
-- Purpose: CPT procedure codes
-- Grain: One row per procedure code
-- ============================================================
CREATE TABLE dim_procedure (
    procedure_key INT PRIMARY KEY AUTO_INCREMENT,
    procedure_id INT NOT NULL,             -- Natural key from OLTP
    cpt_code VARCHAR(10) NOT NULL,
    cpt_description VARCHAR(200),
    
    -- Derived categorizations
    procedure_category VARCHAR(100),       -- Could be derived from code
    
    UNIQUE INDEX idx_procedure_id (procedure_id),
    INDEX idx_cpt_code (cpt_code)
);


-- ============================================================
-- FACT TABLE: fact_encounters
-- Purpose: Central fact table for healthcare encounters
-- Grain: One row per encounter
-- ============================================================
CREATE TABLE fact_encounters (
    -- Surrogate key
    encounter_key INT PRIMARY KEY AUTO_INCREMENT,
    
    -- Natural key from OLTP
    encounter_id INT NOT NULL,
    
    -- Foreign keys to dimensions
    date_key INT NOT NULL,                 -- Encounter date
    discharge_date_key INT,                -- Discharge date
    patient_key INT NOT NULL,
    provider_key INT NOT NULL,
    department_key INT NOT NULL,
    encounter_type_key INT NOT NULL,
    
    -- Degenerate dimensions (no separate table needed)
    encounter_date DATETIME,               -- Keep for precise time
    discharge_date DATETIME,
    
    -- Pre-aggregated metrics (from junction tables)
    diagnosis_count INT DEFAULT 0,
    procedure_count INT DEFAULT 0,
    
    -- Pre-aggregated metrics (from billing)
    total_claim_amount DECIMAL(12,2) DEFAULT 0,
    total_allowed_amount DECIMAL(12,2) DEFAULT 0,
    claim_status VARCHAR(50),
    
    -- Calculated metrics
    length_of_stay_days INT,               -- Derived from dates
    
    -- Foreign key constraints
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (discharge_date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (patient_key) REFERENCES dim_patient(patient_key),
    FOREIGN KEY (provider_key) REFERENCES dim_provider(provider_key),
    FOREIGN KEY (department_key) REFERENCES dim_department(department_key),
    FOREIGN KEY (encounter_type_key) REFERENCES dim_encounter_type(encounter_type_key),
    
    -- Indexes for common query patterns
    UNIQUE INDEX idx_encounter_id (encounter_id),
    INDEX idx_date_key (date_key),
    INDEX idx_patient_key (patient_key),
    INDEX idx_provider_key (provider_key),
    INDEX idx_encounter_type_key (encounter_type_key),
    INDEX idx_date_specialty (date_key, provider_key)  -- For Query 1
);


-- ============================================================
-- BRIDGE TABLE: bridge_encounter_diagnosis
-- Purpose: Many-to-many relationship between encounters and diagnoses
-- Grain: One row per encounter-diagnosis combination
-- ============================================================
CREATE TABLE bridge_encounter_diagnosis (
    bridge_id INT PRIMARY KEY AUTO_INCREMENT,
    encounter_key INT NOT NULL,
    diagnosis_key INT NOT NULL,
    diagnosis_sequence INT,                -- Primary, secondary, etc.
    
    FOREIGN KEY (encounter_key) REFERENCES fact_encounters(encounter_key),
    FOREIGN KEY (diagnosis_key) REFERENCES dim_diagnosis(diagnosis_key),
    
    INDEX idx_encounter_key (encounter_key),
    INDEX idx_diagnosis_key (diagnosis_key),
    UNIQUE INDEX idx_enc_diag (encounter_key, diagnosis_key)
);


-- ============================================================
-- BRIDGE TABLE: bridge_encounter_procedure
-- Purpose: Many-to-many relationship between encounters and procedures
-- Grain: One row per encounter-procedure combination
-- ============================================================
CREATE TABLE bridge_encounter_procedure (
    bridge_id INT PRIMARY KEY AUTO_INCREMENT,
    encounter_key INT NOT NULL,
    procedure_key INT NOT NULL,
    procedure_date DATE,
    
    FOREIGN KEY (encounter_key) REFERENCES fact_encounters(encounter_key),
    FOREIGN KEY (procedure_key) REFERENCES dim_procedure(procedure_key),
    
    INDEX idx_encounter_key (encounter_key),
    INDEX idx_procedure_key (procedure_key),
    UNIQUE INDEX idx_enc_proc (encounter_key, procedure_key)
);


-- ============================================================
-- POPULATE dim_date (2023-2025)
-- One-time load for date dimension
-- Using a stored procedure to avoid recursion limits
-- ============================================================

DELIMITER //
CREATE PROCEDURE populate_dim_date()
BEGIN
    DECLARE v_date DATE DEFAULT '2023-01-01';
    DECLARE v_end_date DATE DEFAULT '2025-12-31';
    
    WHILE v_date <= v_end_date DO
        INSERT INTO dim_date (
            date_key, calendar_date, year, quarter, month, 
            month_name, week_of_year, day_of_month, day_of_week, 
            day_name, is_weekend, fiscal_year, fiscal_quarter
        ) VALUES (
            CAST(DATE_FORMAT(v_date, '%Y%m%d') AS UNSIGNED),
            v_date,
            YEAR(v_date),
            QUARTER(v_date),
            MONTH(v_date),
            MONTHNAME(v_date),
            WEEK(v_date),
            DAY(v_date),
            DAYOFWEEK(v_date),
            DAYNAME(v_date),
            DAYOFWEEK(v_date) IN (1, 7),
            YEAR(v_date),
            QUARTER(v_date)
        );
        SET v_date = DATE_ADD(v_date, INTERVAL 1 DAY);
    END WHILE;
END //
DELIMITER ;

-- Execute the procedure
CALL populate_dim_date();

-- Drop the procedure after use
DROP PROCEDURE IF EXISTS populate_dim_date;


-- ============================================================
-- POPULATE dim_encounter_type
-- Static dimension - small lookup table
-- ============================================================
INSERT INTO dim_encounter_type (encounter_type_name, encounter_type_category) VALUES
    ('Outpatient', 'Ambulatory'),
    ('Inpatient', 'Acute Care'),
    ('ER', 'Emergency');


-- ============================================================
-- VERIFICATION: Check table structure
-- ============================================================
SELECT 
    TABLE_NAME,
    TABLE_ROWS,
    ROUND(DATA_LENGTH / 1024 / 1024, 2) AS data_size_mb
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = DATABASE()
AND TABLE_NAME LIKE 'dim_%' OR TABLE_NAME LIKE 'fact_%' OR TABLE_NAME LIKE 'bridge_%'
ORDER BY TABLE_NAME;
