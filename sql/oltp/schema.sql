-- ============================================================
-- Healthcare Analytics: OLTP Schema (3NF Normalized)
-- Operational Transactional Database Definition
-- ============================================================

-- Drop tables if they exist (for re-running script)
DROP TABLE IF EXISTS billing;
DROP TABLE IF EXISTS encounter_procedures;
DROP TABLE IF EXISTS encounter_diagnoses;
DROP TABLE IF EXISTS encounters;
DROP TABLE IF EXISTS procedures;
DROP TABLE IF EXISTS diagnoses;
DROP TABLE IF EXISTS providers;
DROP TABLE IF EXISTS departments;
DROP TABLE IF EXISTS specialties;
DROP TABLE IF EXISTS patients;

-- ============================================================
-- TABLE 1: patients
-- Stores patient demographics and Medical Record Number (MRN)
-- ============================================================
CREATE TABLE patients (
    patient_id INT PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    date_of_birth DATE,
    gender CHAR(1),
    mrn VARCHAR(20) UNIQUE
);

-- ============================================================
-- TABLE 2: specialties
-- Medical specialties lookup table
-- ============================================================
CREATE TABLE specialties (
    specialty_id INT PRIMARY KEY,
    specialty_name VARCHAR(100),
    specialty_code VARCHAR(10)
);

-- ============================================================
-- TABLE 3: departments
-- Hospital departments with floor and capacity info
-- ============================================================
CREATE TABLE departments (
    department_id INT PRIMARY KEY,
    department_name VARCHAR(100),
    floor INT,
    capacity INT
);

-- ============================================================
-- TABLE 4: providers
-- Healthcare providers (doctors) with specialty and department
-- ============================================================
CREATE TABLE providers (
    provider_id INT PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    credential VARCHAR(20),
    specialty_id INT,
    department_id INT,
    FOREIGN KEY (specialty_id) REFERENCES specialties(specialty_id),
    FOREIGN KEY (department_id) REFERENCES departments(department_id)
);

-- ============================================================
-- TABLE 5: encounters
-- Patient visits/encounters with providers
-- Types: Outpatient, Inpatient, ER
-- ============================================================
CREATE TABLE encounters (
    encounter_id INT PRIMARY KEY,
    patient_id INT,
    provider_id INT,
    encounter_type VARCHAR(50),
    encounter_date DATETIME,
    discharge_date DATETIME,
    department_id INT,
    FOREIGN KEY (patient_id) REFERENCES patients(patient_id),
    FOREIGN KEY (provider_id) REFERENCES providers(provider_id),
    FOREIGN KEY (department_id) REFERENCES departments(department_id)
);

-- Create index on encounter_date for performance
CREATE INDEX idx_encounter_date ON encounters(encounter_date);

-- ============================================================
-- TABLE 6: diagnoses
-- ICD-10 diagnosis codes lookup table
-- ============================================================
CREATE TABLE diagnoses (
    diagnosis_id INT PRIMARY KEY,
    icd10_code VARCHAR(10),
    icd10_description VARCHAR(200)
);

-- ============================================================
-- TABLE 7: encounter_diagnoses (Junction Table)
-- Many-to-many relationship: encounters <-> diagnoses
-- ============================================================
CREATE TABLE encounter_diagnoses (
    encounter_diagnosis_id INT PRIMARY KEY,
    encounter_id INT,
    diagnosis_id INT,
    diagnosis_sequence INT,
    FOREIGN KEY (encounter_id) REFERENCES encounters(encounter_id),
    FOREIGN KEY (diagnosis_id) REFERENCES diagnoses(diagnosis_id)
);

-- ============================================================
-- TABLE 8: procedures
-- CPT procedure codes lookup table
-- ============================================================
CREATE TABLE procedures (
    procedure_id INT PRIMARY KEY,
    cpt_code VARCHAR(10),
    cpt_description VARCHAR(200)
);

-- ============================================================
-- TABLE 9: encounter_procedures (Junction Table)
-- Many-to-many relationship: encounters <-> procedures
-- ============================================================
CREATE TABLE encounter_procedures (
    encounter_procedure_id INT PRIMARY KEY,
    encounter_id INT,
    procedure_id INT,
    procedure_date DATE,
    FOREIGN KEY (encounter_id) REFERENCES encounters(encounter_id),
    FOREIGN KEY (procedure_id) REFERENCES procedures(procedure_id)
);

-- ============================================================
-- TABLE 10: billing
-- Billing/claims data for encounters
-- ============================================================
CREATE TABLE billing (
    billing_id INT PRIMARY KEY,
    encounter_id INT,
    claim_amount DECIMAL(12,2),
    allowed_amount DECIMAL(12,2),
    claim_date DATE,
    claim_status VARCHAR(50),
    FOREIGN KEY (encounter_id) REFERENCES encounters(encounter_id)
);

-- Create index on claim_date for performance
CREATE INDEX idx_claim_date ON billing(claim_date);

-- ============================================================
-- SAMPLE DATA INSERTION
-- ============================================================

-- Insert Specialties
INSERT INTO specialties VALUES
    (1, 'Cardiology', 'CARD'),
    (2, 'Internal Medicine', 'IM'),
    (3, 'Emergency', 'ER');

-- Insert Departments
INSERT INTO departments VALUES
    (1, 'Cardiology Unit', 3, 20),
    (2, 'Internal Medicine', 2, 30),
    (3, 'Emergency', 1, 45);

-- Insert Providers (Doctors)
INSERT INTO providers VALUES
    (101, 'James', 'Chen', 'MD', 1, 1),
    (102, 'Sarah', 'Williams', 'MD', 2, 2),
    (103, 'Michael', 'Rodriguez', 'MD', 3, 3);

-- Insert Patients
INSERT INTO patients VALUES
    (1001, 'John', 'Doe', '1955-03-15', 'M', 'MRN001'),
    (1002, 'Jane', 'Smith', '1962-07-22', 'F', 'MRN002'),
    (1003, 'Robert', 'Johnson', '1948-11-08', 'M', 'MRN003');

-- Insert Diagnoses (ICD-10 codes)
INSERT INTO diagnoses VALUES
    (3001, 'I10', 'Hypertension'),
    (3002, 'E11.9', 'Type 2 Diabetes'),
    (3003, 'I50.9', 'Heart Failure');

-- Insert Procedures (CPT codes)
INSERT INTO procedures VALUES
    (4001, '99213', 'Office Visit'),
    (4002, '93000', 'EKG'),
    (4003, '71020', 'Chest X-ray');

-- Insert Encounters
INSERT INTO encounters VALUES
    (7001, 1001, 101, 'Outpatient', '2024-05-10 10:00:00', '2024-05-10 11:30:00', 1),
    (7002, 1001, 101, 'Inpatient', '2024-06-02 14:00:00', '2024-06-06 09:00:00', 1),
    (7003, 1002, 102, 'Outpatient', '2024-05-15 09:00:00', '2024-05-15 10:15:00', 2),
    (7004, 1003, 103, 'ER', '2024-06-12 23:45:00', '2024-06-13 06:30:00', 3);

-- Insert Encounter-Diagnoses (Junction table)
INSERT INTO encounter_diagnoses VALUES
    (8001, 7001, 3001, 1),  -- Encounter 7001: Hypertension (primary)
    (8002, 7001, 3002, 2),  -- Encounter 7001: Type 2 Diabetes (secondary)
    (8003, 7002, 3001, 1),  -- Encounter 7002: Hypertension (primary)
    (8004, 7002, 3003, 2),  -- Encounter 7002: Heart Failure (secondary)
    (8005, 7003, 3002, 1),  -- Encounter 7003: Type 2 Diabetes (primary)
    (8006, 7004, 3001, 1);  -- Encounter 7004: Hypertension (primary)

-- Insert Encounter-Procedures (Junction table)
INSERT INTO encounter_procedures VALUES
    (9001, 7001, 4001, '2024-05-10'),  -- Encounter 7001: Office Visit
    (9002, 7001, 4002, '2024-05-10'),  -- Encounter 7001: EKG
    (9003, 7002, 4001, '2024-06-02'),  -- Encounter 7002: Office Visit
    (9004, 7003, 4001, '2024-05-15');  -- Encounter 7003: Office Visit

-- Insert Billing
    INSERT INTO billing VALUES
    (14001, 7001, 350.00, 280.00, '2024-05-11', 'Paid'),
    (14002, 7002, 12500.00, 10000.00, '2024-06-08', 'Paid');

-- ============================================================
-- VERIFICATION QUERIES
-- ============================================================

-- Count records in each table
SELECT 'patients' as table_name, COUNT(*) as row_count FROM patients
UNION ALL SELECT 'specialties', COUNT(*) FROM specialties
UNION ALL SELECT 'departments', COUNT(*) FROM departments
UNION ALL SELECT 'providers', COUNT(*) FROM providers
UNION ALL SELECT 'encounters', COUNT(*) FROM encounters
UNION ALL SELECT 'diagnoses', COUNT(*) FROM diagnoses
UNION ALL SELECT 'encounter_diagnoses', COUNT(*) FROM encounter_diagnoses
UNION ALL SELECT 'procedures', COUNT(*) FROM procedures
UNION ALL SELECT 'encounter_procedures', COUNT(*) FROM encounter_procedures
UNION ALL SELECT 'billing', COUNT(*) FROM billing;
