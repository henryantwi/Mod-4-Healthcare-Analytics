"""
Healthcare Analytics ETL Pipeline
Extracts data from OLTP, transforms attributes, and loads into Star Schema.

Features:
- Incremental loading (only new/changed records since last load)
- SCD Type 2 for dim_patient and dim_provider (tracks history)
- SCD Type 1 for other dimensions (overwrite)
- Late-arriving fact handling for billing data
"""

import mysql.connector
from mysql.connector import Error
from datetime import datetime, date

# Configuration - matches docker-compose.yml and .env settings
DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'root_password',
    'database': 'healthcare_db'
}

# Default timestamp for first-ever load (loads everything)
INITIAL_LOAD_TIMESTAMP = '1900-01-01 00:00:00'


def get_connection():
    """Create database connection"""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None


def get_last_load_timestamp(cursor, table_name):
    """Get the last load timestamp for a table from etl_metadata"""
    cursor.execute("""
        SELECT last_load_timestamp FROM etl_metadata WHERE table_name = %s
    """, (table_name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    return datetime.strptime(INITIAL_LOAD_TIMESTAMP, '%Y-%m-%d %H:%M:%S')


def update_etl_metadata(cursor, table_name, records_loaded, load_type='INCREMENTAL'):
    """Update the ETL metadata after a successful load"""
    cursor.execute("""
        INSERT INTO etl_metadata (table_name, last_load_timestamp, records_loaded, load_type)
        VALUES (%s, NOW(), %s, %s)
        ON DUPLICATE KEY UPDATE 
            last_load_timestamp = NOW(),
            records_loaded = %s,
            load_type = %s
    """, (table_name, records_loaded, load_type, records_loaded, load_type))


def load_dim_patient(cursor):
    """
    Load patient dimension with SCD Type 2 logic.
    - New patients: INSERT with is_current=TRUE
    - Changed patients: Close old row, INSERT new row
    - Unchanged: Skip
    """
    print("Loading dim_patient (SCD Type 2)...")
    
    last_load = get_last_load_timestamp(cursor, 'dim_patient')
    today = date.today()
    
    # Get new/changed patients from OLTP
    cursor.execute("""
        SELECT 
            patient_id, first_name, last_name, date_of_birth, gender, mrn,
            CONCAT(first_name, ' ', last_name) AS full_name,
            CASE gender WHEN 'M' THEN 'Male' WHEN 'F' THEN 'Female' ELSE 'Unknown' END AS gender_description,
            TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE()) AS age,
            CASE 
                WHEN TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE()) < 18 THEN '0-17'
                WHEN TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE()) < 35 THEN '18-34'
                WHEN TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE()) < 55 THEN '35-54'
                WHEN TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE()) < 75 THEN '55-74'
                ELSE '75+'
            END AS age_group
        FROM patients
        WHERE updated_at >= %s OR created_at >= %s
    """, (last_load, last_load))
    
    patients = cursor.fetchall()
    records_processed = 0
    
    for patient in patients:
        patient_id = patient[0]
        
        # Check if patient already exists in dimension
        cursor.execute("""
            SELECT patient_key, first_name, last_name, date_of_birth, gender, mrn
            FROM dim_patient 
            WHERE patient_id = %s AND is_current = TRUE
        """, (patient_id,))
        existing = cursor.fetchone()
        
        if existing is None:
            # NEW patient - insert
            cursor.execute("""
                INSERT INTO dim_patient (
                    patient_id, first_name, last_name, full_name, date_of_birth,
                    gender, gender_description, age, age_group, mrn,
                    effective_date, end_date, is_current
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, '9999-12-31', TRUE)
            """, (patient[0], patient[1], patient[2], patient[6], patient[3],
                  patient[4], patient[7], patient[8], patient[9], patient[5], today))
            records_processed += 1
        else:
            # Check if anything changed (SCD Type 2)
            if (existing[1] != patient[1] or existing[2] != patient[2] or 
                existing[3] != patient[3] or existing[4] != patient[4] or 
                existing[5] != patient[5]):
                
                # Close old record
                cursor.execute("""
                    UPDATE dim_patient 
                    SET end_date = %s, is_current = FALSE
                    WHERE patient_key = %s
                """, (today, existing[0]))
                
                # Insert new version
                cursor.execute("""
                    INSERT INTO dim_patient (
                        patient_id, first_name, last_name, full_name, date_of_birth,
                        gender, gender_description, age, age_group, mrn,
                        effective_date, end_date, is_current
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, '9999-12-31', TRUE)
                """, (patient[0], patient[1], patient[2], patient[6], patient[3],
                      patient[4], patient[7], patient[8], patient[9], patient[5], today))
                records_processed += 1
    
    update_etl_metadata(cursor, 'dim_patient', records_processed)
    print(f"  Processed {records_processed} patients (new/changed)")


def load_dim_provider(cursor):
    """
    Load provider dimension with SCD Type 2 logic.
    Critical for tracking specialty changes over time.
    """
    print("Loading dim_provider (SCD Type 2)...")
    
    last_load = get_last_load_timestamp(cursor, 'dim_provider')
    today = date.today()
    
    # Get new/changed providers (with denormalized specialty & department)
    cursor.execute("""
        SELECT 
            p.provider_id, p.first_name, p.last_name,
            CONCAT(p.first_name, ' ', p.last_name) AS full_name,
            p.credential,
            s.specialty_id, s.specialty_name, s.specialty_code,
            d.department_id, d.department_name
        FROM providers p
        JOIN specialties s ON p.specialty_id = s.specialty_id
        JOIN departments d ON p.department_id = d.department_id
        WHERE p.updated_at >= %s OR p.created_at >= %s
    """, (last_load, last_load))
    
    providers = cursor.fetchall()
    records_processed = 0
    
    for provider in providers:
        provider_id = provider[0]
        
        # Check if provider already exists
        cursor.execute("""
            SELECT provider_key, first_name, last_name, credential, specialty_id, department_id
            FROM dim_provider 
            WHERE provider_id = %s AND is_current = TRUE
        """, (provider_id,))
        existing = cursor.fetchone()
        
        if existing is None:
            # NEW provider - insert
            cursor.execute("""
                INSERT INTO dim_provider (
                    provider_id, first_name, last_name, full_name, credential,
                    specialty_id, specialty_name, specialty_code,
                    department_id, department_name,
                    effective_date, end_date, is_current
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, '9999-12-31', TRUE)
            """, (provider[0], provider[1], provider[2], provider[3], provider[4],
                  provider[5], provider[6], provider[7], provider[8], provider[9], today))
            records_processed += 1
        else:
            # Check if specialty or department changed (SCD Type 2)
            if existing[4] != provider[5] or existing[5] != provider[8]:
                # Close old record
                cursor.execute("""
                    UPDATE dim_provider 
                    SET end_date = %s, is_current = FALSE
                    WHERE provider_key = %s
                """, (today, existing[0]))
                
                # Insert new version
                cursor.execute("""
                    INSERT INTO dim_provider (
                        provider_id, first_name, last_name, full_name, credential,
                        specialty_id, specialty_name, specialty_code,
                        department_id, department_name,
                        effective_date, end_date, is_current
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, '9999-12-31', TRUE)
                """, (provider[0], provider[1], provider[2], provider[3], provider[4],
                      provider[5], provider[6], provider[7], provider[8], provider[9], today))
                records_processed += 1
    
    update_etl_metadata(cursor, 'dim_provider', records_processed)
    print(f"  Processed {records_processed} providers (new/changed)")


def load_dim_department(cursor):
    """Load department dimension (SCD Type 1 - upsert)"""
    print("Loading dim_department (SCD Type 1)...")
    
    last_load = get_last_load_timestamp(cursor, 'dim_department')
    
    cursor.execute("""
        INSERT INTO dim_department (department_id, department_name, floor, capacity)
        SELECT department_id, department_name, floor, capacity
        FROM departments
        WHERE updated_at >= %s OR created_at >= %s
        ON DUPLICATE KEY UPDATE
            department_name = VALUES(department_name),
            floor = VALUES(floor),
            capacity = VALUES(capacity)
    """, (last_load, last_load))
    
    update_etl_metadata(cursor, 'dim_department', cursor.rowcount)
    print(f"  Processed {cursor.rowcount} departments")


def load_dim_diagnosis(cursor):
    """Load diagnosis dimension (SCD Type 1 - upsert)"""
    print("Loading dim_diagnosis (SCD Type 1)...")
    
    last_load = get_last_load_timestamp(cursor, 'dim_diagnosis')
    
    cursor.execute("""
        INSERT INTO dim_diagnosis (diagnosis_id, icd10_code, icd10_description, diagnosis_category)
        SELECT 
            diagnosis_id,
            icd10_code,
            icd10_description,
            CASE 
                WHEN icd10_code LIKE 'I%%' THEN 'Circulatory System'
                WHEN icd10_code LIKE 'E%%' THEN 'Endocrine/Metabolic'
                WHEN icd10_code LIKE 'J%%' THEN 'Respiratory System'
                WHEN icd10_code LIKE 'M%%' THEN 'Musculoskeletal'
                WHEN icd10_code LIKE 'K%%' THEN 'Digestive System'
                WHEN icd10_code LIKE 'N%%' THEN 'Genitourinary'
                WHEN icd10_code LIKE 'F%%' THEN 'Mental/Behavioral'
                WHEN icd10_code LIKE 'G%%' THEN 'Nervous System'
                WHEN icd10_code LIKE 'C%%' THEN 'Neoplasms'
                ELSE 'Other'
            END AS diagnosis_category
        FROM diagnoses
        WHERE updated_at >= %s OR created_at >= %s
        ON DUPLICATE KEY UPDATE
            icd10_code = VALUES(icd10_code),
            icd10_description = VALUES(icd10_description),
            diagnosis_category = VALUES(diagnosis_category)
    """, (last_load, last_load))
    
    update_etl_metadata(cursor, 'dim_diagnosis', cursor.rowcount)
    print(f"  Processed {cursor.rowcount} diagnoses")


def load_dim_procedure(cursor):
    """Load procedure dimension (SCD Type 1 - upsert)"""
    print("Loading dim_procedure (SCD Type 1)...")
    
    last_load = get_last_load_timestamp(cursor, 'dim_procedure')
    
    cursor.execute("""
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
        WHERE updated_at >= %s OR created_at >= %s
        ON DUPLICATE KEY UPDATE
            cpt_code = VALUES(cpt_code),
            cpt_description = VALUES(cpt_description),
            procedure_category = VALUES(procedure_category)
    """, (last_load, last_load))
    
    update_etl_metadata(cursor, 'dim_procedure', cursor.rowcount)
    print(f"  Processed {cursor.rowcount} procedures")


def load_fact_encounters(cursor):
    """Load fact table with incremental logic and pre-aggregated metrics"""
    print("Loading fact_encounters (incremental)...")
    
    last_load = get_last_load_timestamp(cursor, 'fact_encounters')
    
    # Load only new/changed encounters
    cursor.execute("""
        INSERT INTO fact_encounters (
            encounter_id, date_key, discharge_date_key, patient_key, provider_key,
            department_key, encounter_type_key, encounter_date, discharge_date,
            diagnosis_count, procedure_count, total_claim_amount, total_allowed_amount,
            claim_status, length_of_stay_days
        )
        SELECT 
            e.encounter_id,
            CAST(DATE_FORMAT(e.encounter_date, '%%Y%%m%%d') AS UNSIGNED) AS date_key,
            CAST(DATE_FORMAT(e.discharge_date, '%%Y%%m%%d') AS UNSIGNED) AS discharge_date_key,
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
        -- Join to dimensions (use is_current for SCD Type 2)
        JOIN dim_patient dp ON e.patient_id = dp.patient_id AND dp.is_current = TRUE
        JOIN dim_provider dpr ON e.provider_id = dpr.provider_id AND dpr.is_current = TRUE
        JOIN dim_department dd ON e.department_id = dd.department_id
        JOIN dim_encounter_type det ON e.encounter_type = det.encounter_type_name
        -- Left join for optional data
        LEFT JOIN billing b ON e.encounter_id = b.encounter_id
        -- Pre-aggregate diagnosis count
        LEFT JOIN (
            SELECT encounter_id, COUNT(*) AS diagnosis_count
            FROM encounter_diagnoses GROUP BY encounter_id
        ) diag_counts ON e.encounter_id = diag_counts.encounter_id
        -- Pre-aggregate procedure count
        LEFT JOIN (
            SELECT encounter_id, COUNT(*) AS procedure_count
            FROM encounter_procedures GROUP BY encounter_id
        ) proc_counts ON e.encounter_id = proc_counts.encounter_id
        WHERE e.updated_at >= %s OR e.created_at >= %s
        ON DUPLICATE KEY UPDATE
            diagnosis_count = VALUES(diagnosis_count),
            procedure_count = VALUES(procedure_count),
            total_claim_amount = VALUES(total_claim_amount),
            total_allowed_amount = VALUES(total_allowed_amount),
            claim_status = VALUES(claim_status)
    """, (last_load, last_load))
    
    update_etl_metadata(cursor, 'fact_encounters', cursor.rowcount)
    print(f"  Processed {cursor.rowcount} encounters")


def update_late_arriving_billing(cursor):
    """Update fact table for billing that arrived after encounter was loaded"""
    print("Updating late-arriving billing...")
    
    last_load = get_last_load_timestamp(cursor, 'billing_updates')
    
    cursor.execute("""
        UPDATE fact_encounters f
        JOIN billing b ON f.encounter_id = b.encounter_id
        SET 
            f.total_claim_amount = b.claim_amount,
            f.total_allowed_amount = b.allowed_amount,
            f.claim_status = b.claim_status
        WHERE f.total_claim_amount = 0 
          AND b.claim_amount > 0
          AND (b.updated_at >= %s OR b.created_at >= %s)
    """, (last_load, last_load))
    
    update_etl_metadata(cursor, 'billing_updates', cursor.rowcount)
    print(f"  Updated {cursor.rowcount} encounters with late billing")


def load_bridge_encounter_diagnosis(cursor):
    """Load bridge table for new encounters only"""
    print("Loading bridge_encounter_diagnosis...")
    
    last_load = get_last_load_timestamp(cursor, 'bridge_encounter_diagnosis')
    
    cursor.execute("""
        INSERT IGNORE INTO bridge_encounter_diagnosis (encounter_key, diagnosis_key, diagnosis_sequence)
        SELECT 
            fe.encounter_key,
            dd.diagnosis_key,
            ed.diagnosis_sequence
        FROM encounter_diagnoses ed
        JOIN fact_encounters fe ON ed.encounter_id = fe.encounter_id
        JOIN dim_diagnosis dd ON ed.diagnosis_id = dd.diagnosis_id
        JOIN encounters e ON ed.encounter_id = e.encounter_id
        WHERE e.updated_at >= %s OR e.created_at >= %s
    """, (last_load, last_load))
    
    update_etl_metadata(cursor, 'bridge_encounter_diagnosis', cursor.rowcount)
    print(f"  Processed {cursor.rowcount} diagnosis links")


def load_bridge_encounter_procedure(cursor):
    """Load bridge table for new encounters only"""
    print("Loading bridge_encounter_procedure...")
    
    last_load = get_last_load_timestamp(cursor, 'bridge_encounter_procedure')
    
    cursor.execute("""
        INSERT IGNORE INTO bridge_encounter_procedure (encounter_key, procedure_key, procedure_date)
        SELECT 
            fe.encounter_key,
            dpc.procedure_key,
            ep.procedure_date
        FROM encounter_procedures ep
        JOIN fact_encounters fe ON ep.encounter_id = fe.encounter_id
        JOIN dim_procedure dpc ON ep.procedure_id = dpc.procedure_id
        JOIN encounters e ON ep.encounter_id = e.encounter_id
        WHERE e.updated_at >= %s OR e.created_at >= %s
    """, (last_load, last_load))
    
    update_etl_metadata(cursor, 'bridge_encounter_procedure', cursor.rowcount)
    print(f"  Processed {cursor.rowcount} procedure links")


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
    
    # Show ETL metadata
    print("\n" + "-" * 60)
    print("ETL Metadata (Last Load Times)")
    print("-" * 60)
    cursor.execute("SELECT table_name, last_load_timestamp, records_loaded FROM etl_metadata ORDER BY table_name")
    for row in cursor.fetchall():
        print(f"  {row[0]:35} {str(row[1]):20} ({row[2]} records)")


def run_etl():
    """Main ETL function - runs incremental load"""
    print("=" * 60)
    print("ETL Pipeline Execution (INCREMENTAL)")
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
        
        # Step 3: Update late-arriving billing
        print()
        print("STEP 3: Late-Arriving Facts")
        print("-" * 40)
        update_late_arriving_billing(cursor)
        connection.commit()
        
        # Step 4: Load bridge tables
        print()
        print("STEP 4: Loading Bridge Tables")
        print("-" * 40)
        load_bridge_encounter_diagnosis(cursor)
        connection.commit()
        
        load_bridge_encounter_procedure(cursor)
        connection.commit()
        
        # Step 5: Verify
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