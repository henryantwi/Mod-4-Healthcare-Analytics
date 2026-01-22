"""
Healthcare Analytics ETL Pipeline
Extracts data from OLTP, transforms attributes, and loads into Star Schema dimensions and facts.
"""

import mysql.connector
from mysql.connector import Error
from datetime import datetime

# Configuration - matches docker-compose.yml and .env settings
DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'root_password',
    'database': 'healthcare_db'
}


def get_connection():
    """Create database connection"""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None


def load_dim_patient(cursor):
    """Load patient dimension with derived attributes"""
    print("Loading dim_patient...")
    
    query = """
    INSERT INTO dim_patient (patient_id, first_name, last_name, full_name, 
                             date_of_birth, gender, gender_description, 
                             age, age_group, mrn)
    SELECT 
        patient_id,
        first_name,
        last_name,
        CONCAT(first_name, ' ', last_name) AS full_name,
        date_of_birth,
        gender,
        CASE gender WHEN 'M' THEN 'Male' WHEN 'F' THEN 'Female' ELSE 'Unknown' END AS gender_description,
        TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE()) AS age,
        CASE 
            WHEN TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE()) < 18 THEN '0-17'
            WHEN TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE()) < 35 THEN '18-34'
            WHEN TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE()) < 55 THEN '35-54'
            WHEN TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE()) < 75 THEN '55-74'
            ELSE '75+'
        END AS age_group,
        mrn
    FROM patients
    """
    cursor.execute(query)
    print(f"  Loaded {cursor.rowcount} patients into dim_patient")


def load_dim_provider(cursor):
    """Load provider dimension with denormalized specialty and department"""
    print("Loading dim_provider...")
    
    query = """
    INSERT INTO dim_provider (provider_id, first_name, last_name, full_name,
                              credential, specialty_id, specialty_name, 
                              specialty_code, department_id, department_name)
    SELECT 
        p.provider_id,
        p.first_name,
        p.last_name,
        CONCAT(p.first_name, ' ', p.last_name) AS full_name,
        p.credential,
        s.specialty_id,
        s.specialty_name,
        s.specialty_code,
        d.department_id,
        d.department_name
    FROM providers p
    JOIN specialties s ON p.specialty_id = s.specialty_id
    JOIN departments d ON p.department_id = d.department_id
    """
    cursor.execute(query)
    print(f"  Loaded {cursor.rowcount} providers into dim_provider")


def load_dim_department(cursor):
    """Load department dimension"""
    print("Loading dim_department...")
    
    query = """
    INSERT INTO dim_department (department_id, department_name, floor, capacity)
    SELECT department_id, department_name, floor, capacity
    FROM departments
    """
    cursor.execute(query)
    print(f"  Loaded {cursor.rowcount} departments into dim_department")


def load_dim_diagnosis(cursor):
    """Load diagnosis dimension"""
    print("Loading dim_diagnosis...")
    
    query = """
    INSERT INTO dim_diagnosis (diagnosis_id, icd10_code, icd10_description, diagnosis_category)
    SELECT 
        diagnosis_id,
        icd10_code,
        icd10_description,
        CASE 
            WHEN icd10_code LIKE 'I%' THEN 'Circulatory System'
            WHEN icd10_code LIKE 'E%' THEN 'Endocrine/Metabolic'
            WHEN icd10_code LIKE 'J%' THEN 'Respiratory System'
            WHEN icd10_code LIKE 'M%' THEN 'Musculoskeletal'
            WHEN icd10_code LIKE 'K%' THEN 'Digestive System'
            WHEN icd10_code LIKE 'N%' THEN 'Genitourinary'
            WHEN icd10_code LIKE 'F%' THEN 'Mental/Behavioral'
            WHEN icd10_code LIKE 'G%' THEN 'Nervous System'
            WHEN icd10_code LIKE 'C%' THEN 'Neoplasms'
            WHEN icd10_code LIKE 'R%' THEN 'Symptoms/Signs'
            WHEN icd10_code LIKE 'S%' THEN 'Injury/Trauma'
            WHEN icd10_code LIKE 'A%' OR icd10_code LIKE 'B%' THEN 'Infectious Disease'
            WHEN icd10_code LIKE 'D%' THEN 'Blood/Immune'
            WHEN icd10_code LIKE 'L%' THEN 'Skin'
            ELSE 'Other'
        END AS diagnosis_category
    FROM diagnoses
    """
    cursor.execute(query)
    print(f"  Loaded {cursor.rowcount} diagnoses into dim_diagnosis")


def load_dim_procedure(cursor):
    """Load procedure dimension"""
    print("Loading dim_procedure...")
    
    query = """
    INSERT INTO dim_procedure (procedure_id, cpt_code, cpt_description, procedure_category)
    SELECT 
        procedure_id,
        cpt_code,
        cpt_description,
        CASE 
            WHEN cpt_code BETWEEN '99201' AND '99499' THEN 'E&M Services'
            WHEN cpt_code BETWEEN '70000' AND '79999' THEN 'Radiology'
            WHEN cpt_code BETWEEN '80000' AND '89999' THEN 'Pathology/Lab'
            WHEN cpt_code BETWEEN '90000' AND '99199' THEN 'Medicine'
            WHEN cpt_code BETWEEN '10000' AND '69999' THEN 'Surgery'
            ELSE 'Other'
        END AS procedure_category
    FROM procedures
    """
    cursor.execute(query)
    print(f"  Loaded {cursor.rowcount} procedures into dim_procedure")


def load_fact_encounters(cursor):
    """Load fact table with pre-aggregated metrics"""
    print("Loading fact_encounters...")
    
    query = """
    INSERT INTO fact_encounters (
        encounter_id, date_key, discharge_date_key, patient_key, provider_key,
        department_key, encounter_type_key, encounter_date, discharge_date,
        diagnosis_count, procedure_count, total_claim_amount, total_allowed_amount,
        claim_status, length_of_stay_days
    )
    SELECT 
        e.encounter_id,
        CAST(DATE_FORMAT(e.encounter_date, '%Y%m%d') AS UNSIGNED) AS date_key,
        CAST(DATE_FORMAT(e.discharge_date, '%Y%m%d') AS UNSIGNED) AS discharge_date_key,
        dp.patient_key,
        dpr.provider_key,
        dd.department_key,
        det.encounter_type_key,
        e.encounter_date,
        e.discharge_date,
        COALESCE(diag_counts.diagnosis_count, 0) AS diagnosis_count,
        COALESCE(proc_counts.procedure_count, 0) AS procedure_count,
        COALESCE(b.claim_amount, 0) AS total_claim_amount,
        COALESCE(b.allowed_amount, 0) AS total_allowed_amount,
        b.claim_status,
        DATEDIFF(e.discharge_date, e.encounter_date) AS length_of_stay_days
    FROM encounters e
    -- Join to dimensions
    JOIN dim_patient dp ON e.patient_id = dp.patient_id
    JOIN dim_provider dpr ON e.provider_id = dpr.provider_id
    JOIN dim_department dd ON e.department_id = dd.department_id
    JOIN dim_encounter_type det ON e.encounter_type = det.encounter_type_name
    -- Left join for optional data
    LEFT JOIN billing b ON e.encounter_id = b.encounter_id
    -- Pre-aggregate diagnosis count
    LEFT JOIN (
        SELECT encounter_id, COUNT(*) AS diagnosis_count
        FROM encounter_diagnoses
        GROUP BY encounter_id
    ) diag_counts ON e.encounter_id = diag_counts.encounter_id
    -- Pre-aggregate procedure count
    LEFT JOIN (
        SELECT encounter_id, COUNT(*) AS procedure_count
        FROM encounter_procedures
        GROUP BY encounter_id
    ) proc_counts ON e.encounter_id = proc_counts.encounter_id
    """
    cursor.execute(query)
    print(f"  Loaded {cursor.rowcount} encounters into fact_encounters")


def load_bridge_encounter_diagnosis(cursor):
    """Load bridge table for encounter-diagnosis relationships"""
    print("Loading bridge_encounter_diagnosis...")
    
    query = """
    INSERT INTO bridge_encounter_diagnosis (encounter_key, diagnosis_key, diagnosis_sequence)
    SELECT 
        fe.encounter_key,
        dd.diagnosis_key,
        ed.diagnosis_sequence
    FROM encounter_diagnoses ed
    JOIN fact_encounters fe ON ed.encounter_id = fe.encounter_id
    JOIN dim_diagnosis dd ON ed.diagnosis_id = dd.diagnosis_id
    """
    cursor.execute(query)
    print(f"  Loaded {cursor.rowcount} records into bridge_encounter_diagnosis")


def load_bridge_encounter_procedure(cursor):
    """Load bridge table for encounter-procedure relationships"""
    print("Loading bridge_encounter_procedure...")
    
    query = """
    INSERT INTO bridge_encounter_procedure (encounter_key, procedure_key, procedure_date)
    SELECT 
        fe.encounter_key,
        dpc.procedure_key,
        ep.procedure_date
    FROM encounter_procedures ep
    JOIN fact_encounters fe ON ep.encounter_id = fe.encounter_id
    JOIN dim_procedure dpc ON ep.procedure_id = dpc.procedure_id
    """
    cursor.execute(query)
    print(f"  Loaded {cursor.rowcount} records into bridge_encounter_procedure")


def verify_load(cursor):
    """Verify the ETL load with record counts"""
    print("\n" + "=" * 60)
    print("ETL VERIFICATION - Record Counts")
    print("=" * 60)
    
    tables = [
        ('dim_date', 'date_key'),
        ('dim_patient', 'patient_key'),
        ('dim_provider', 'provider_key'),
        ('dim_department', 'department_key'),
        ('dim_encounter_type', 'encounter_type_key'),
        ('dim_diagnosis', 'diagnosis_key'),
        ('dim_procedure', 'procedure_key'),
        ('fact_encounters', 'encounter_key'),
        ('bridge_encounter_diagnosis', 'bridge_id'),
        ('bridge_encounter_procedure', 'bridge_id'),
    ]
    
    for table, key in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"  {table:35} {count:>10,} rows")


def run_etl():
    """Main ETL function"""
    print("=" * 60)
    print("ETL Pipeline Execution")
    print("=" * 60)
    print(f"Started at: {datetime.now()}")
    print()
    
    connection = get_connection()
    if not connection:
        print("Failed to connect to database.")
        return
    
    cursor = connection.cursor()
    
    try:
        # Step 1: Load dimensions (order matters!)
        print("STEP 1: Loading Dimensions")
        print("-" * 40)
        load_dim_patient(cursor)
        connection.commit()
        
        load_dim_provider(cursor)
        connection.commit()
        
        load_dim_department(cursor)
        connection.commit()
        
        load_dim_diagnosis(cursor)
        connection.commit()
        
        load_dim_procedure(cursor)
        connection.commit()
        
        # Step 2: Load fact table
        print()
        print("STEP 2: Loading Fact Table")
        print("-" * 40)
        load_fact_encounters(cursor)
        connection.commit()
        
        # Step 3: Load bridge tables
        print()
        print("STEP 3: Loading Bridge Tables")
        print("-" * 40)
        load_bridge_encounter_diagnosis(cursor)
        connection.commit()
        
        load_bridge_encounter_procedure(cursor)
        connection.commit()
        
        # Step 4: Verify
        verify_load(cursor)
        
        print()
        print("=" * 60)
        print("ETL COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print(f"Finished at: {datetime.now()}")
        
    except Error as e:
        print(f"Error during ETL: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    run_etl()
