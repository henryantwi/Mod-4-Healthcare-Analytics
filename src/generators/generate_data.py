"""
Healthcare Analytics Data Generator
Synthetic data generation script to populate OLTP tables with realistic test data.
Target volume: ~10,000 patients, ~50,000 encounters.
"""

import random
from datetime import datetime, timedelta
import mysql.connector
from mysql.connector import Error

# Configuration - matches docker-compose.yml and .env settings
DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'root_password',  # From .env: MYSQL_ROOT_PASSWORD
    'database': 'healthcare_db'   # From .env: MYSQL_DATABASE
}

# Data pools for realistic data generation
FIRST_NAMES = [
    'James', 'Mary', 'John', 'Patricia', 'Robert', 'Jennifer', 'Michael', 'Linda',
    'William', 'Elizabeth', 'David', 'Barbara', 'Richard', 'Susan', 'Joseph', 'Jessica',
    'Thomas', 'Sarah', 'Charles', 'Karen', 'Christopher', 'Nancy', 'Daniel', 'Lisa',
    'Matthew', 'Betty', 'Anthony', 'Margaret', 'Mark', 'Sandra', 'Donald', 'Ashley',
    'Steven', 'Kimberly', 'Paul', 'Emily', 'Andrew', 'Donna', 'Joshua', 'Michelle',
    'Kenneth', 'Dorothy', 'Kevin', 'Carol', 'Brian', 'Amanda', 'George', 'Melissa',
    'Edward', 'Deborah', 'Ronald', 'Stephanie', 'Timothy', 'Rebecca', 'Jason', 'Sharon'
]

LAST_NAMES = [
    'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis',
    'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson',
    'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin', 'Lee', 'Perez', 'Thompson',
    'White', 'Harris', 'Sanchez', 'Clark', 'Ramirez', 'Lewis', 'Robinson', 'Walker',
    'Young', 'Allen', 'King', 'Wright', 'Scott', 'Torres', 'Nguyen', 'Hill', 'Flores',
    'Green', 'Adams', 'Nelson', 'Baker', 'Hall', 'Rivera', 'Campbell', 'Mitchell', 'Carter'
]

# Extended specialties (10 specialties)
SPECIALTIES = [
    (1, 'Cardiology', 'CARD'),
    (2, 'Internal Medicine', 'IM'),
    (3, 'Emergency Medicine', 'ER'),
    (4, 'Orthopedics', 'ORTH'),
    (5, 'Neurology', 'NEUR'),
    (6, 'Pediatrics', 'PEDS'),
    (7, 'Oncology', 'ONC'),
    (8, 'Pulmonology', 'PULM'),
    (9, 'Gastroenterology', 'GI'),
    (10, 'Endocrinology', 'ENDO')
]

# Extended departments (10 departments)
DEPARTMENTS = [
    (1, 'Cardiology Unit', 3, 20),
    (2, 'Internal Medicine', 2, 30),
    (3, 'Emergency Room', 1, 45),
    (4, 'Orthopedic Surgery', 4, 25),
    (5, 'Neurology Ward', 5, 20),
    (6, 'Pediatric Unit', 2, 35),
    (7, 'Oncology Center', 6, 30),
    (8, 'Pulmonary Unit', 3, 25),
    (9, 'GI Lab', 4, 20),
    (10, 'Diabetes Center', 2, 15)
]

# Extended ICD-10 diagnoses (50 diagnoses)
DIAGNOSES = [
    (1, 'I10', 'Essential hypertension'),
    (2, 'E11.9', 'Type 2 diabetes mellitus without complications'),
    (3, 'I50.9', 'Heart failure, unspecified'),
    (4, 'J18.9', 'Pneumonia, unspecified organism'),
    (5, 'M54.5', 'Low back pain'),
    (6, 'J44.9', 'Chronic obstructive pulmonary disease'),
    (7, 'I25.10', 'Coronary artery disease'),
    (8, 'F32.9', 'Major depressive disorder'),
    (9, 'K21.0', 'Gastroesophageal reflux disease'),
    (10, 'N18.9', 'Chronic kidney disease'),
    (11, 'I48.91', 'Atrial fibrillation'),
    (12, 'E78.5', 'Hyperlipidemia'),
    (13, 'G47.33', 'Obstructive sleep apnea'),
    (14, 'M17.11', 'Primary osteoarthritis, right knee'),
    (15, 'J06.9', 'Acute upper respiratory infection'),
    (16, 'R07.9', 'Chest pain, unspecified'),
    (17, 'K59.00', 'Constipation'),
    (18, 'R51', 'Headache'),
    (19, 'N39.0', 'Urinary tract infection'),
    (20, 'L03.90', 'Cellulitis'),
    (21, 'I21.9', 'Acute myocardial infarction'),
    (22, 'G43.909', 'Migraine'),
    (23, 'J45.909', 'Asthma'),
    (24, 'E03.9', 'Hypothyroidism'),
    (25, 'B34.9', 'Viral infection'),
    (26, 'K50.90', 'Crohn disease'),
    (27, 'K51.90', 'Ulcerative colitis'),
    (28, 'G20', 'Parkinson disease'),
    (29, 'G35', 'Multiple sclerosis'),
    (30, 'C50.919', 'Breast cancer'),
    (31, 'C34.90', 'Lung cancer'),
    (32, 'C18.9', 'Colon cancer'),
    (33, 'D64.9', 'Anemia'),
    (34, 'E66.9', 'Obesity'),
    (35, 'F41.1', 'Generalized anxiety disorder'),
    (36, 'I63.9', 'Cerebral infarction (Stroke)'),
    (37, 'S72.001A', 'Hip fracture'),
    (38, 'S82.001A', 'Tibia fracture'),
    (39, 'M79.3', 'Panniculitis'),
    (40, 'R10.9', 'Abdominal pain'),
    (41, 'R50.9', 'Fever'),
    (42, 'R05', 'Cough'),
    (43, 'R06.02', 'Shortness of breath'),
    (44, 'R42', 'Dizziness'),
    (45, 'R11.10', 'Nausea and vomiting'),
    (46, 'E87.6', 'Hypokalemia'),
    (47, 'E87.1', 'Hyponatremia'),
    (48, 'D69.6', 'Thrombocytopenia'),
    (49, 'J96.00', 'Acute respiratory failure'),
    (50, 'A41.9', 'Sepsis')
]

# Extended CPT procedures (30 procedures)
PROCEDURES = [
    (1, '99213', 'Office visit, established patient, level 3'),
    (2, '99214', 'Office visit, established patient, level 4'),
    (3, '99215', 'Office visit, established patient, level 5'),
    (4, '99223', 'Hospital admission, high complexity'),
    (5, '99232', 'Subsequent hospital care, level 2'),
    (6, '99291', 'Critical care, first hour'),
    (7, '93000', 'Electrocardiogram (EKG)'),
    (8, '71046', 'Chest X-ray, 2 views'),
    (9, '74177', 'CT abdomen and pelvis with contrast'),
    (10, '70553', 'MRI brain with contrast'),
    (11, '93306', 'Echocardiogram'),
    (12, '43239', 'Upper GI endoscopy with biopsy'),
    (13, '45380', 'Colonoscopy with biopsy'),
    (14, '27447', 'Total knee replacement'),
    (15, '27130', 'Total hip replacement'),
    (16, '33533', 'Coronary artery bypass graft'),
    (17, '36415', 'Venipuncture (blood draw)'),
    (18, '80053', 'Comprehensive metabolic panel'),
    (19, '85025', 'Complete blood count'),
    (20, '84443', 'TSH blood test'),
    (21, '82947', 'Glucose blood test'),
    (22, '93010', 'EKG interpretation'),
    (23, '94640', 'Nebulizer treatment'),
    (24, '96372', 'Injection, subcutaneous'),
    (25, '20610', 'Joint injection'),
    (26, '64483', 'Epidural injection'),
    (27, '90834', 'Psychotherapy, 45 minutes'),
    (28, '99285', 'Emergency department visit, high severity'),
    (29, '36556', 'Central venous catheter insertion'),
    (30, '31500', 'Intubation')
]

ENCOUNTER_TYPES = ['Outpatient', 'Inpatient', 'ER']
CLAIM_STATUSES = ['Paid', 'Pending', 'Denied', 'Appealed', 'Partially Paid']
CREDENTIALS = ['MD', 'DO', 'NP', 'PA']


def random_date(start_year=1940, end_year=2005):
    """Generate random date of birth"""
    start_date = datetime(start_year, 1, 1)
    end_date = datetime(end_year, 12, 31)
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    return start_date + timedelta(days=random_days)


def random_encounter_date(start_date=datetime(2023, 1, 1), end_date=datetime(2024, 11, 30)):
    """Generate random encounter date"""
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    random_hours = random.randint(0, 23)
    random_minutes = random.randint(0, 59)
    return start_date + timedelta(days=random_days, hours=random_hours, minutes=random_minutes)


def get_connection():
    """Create database connection"""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None


def clear_existing_data(cursor):
    """Clear existing data from all tables"""
    print("Clearing existing data...")
    tables = [
        'billing', 'encounter_procedures', 'encounter_diagnoses',
        'encounters', 'procedures', 'diagnoses', 'providers',
        'departments', 'specialties', 'patients'
    ]
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
    for table in tables:
        cursor.execute(f"TRUNCATE TABLE {table}")
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
    print("Existing data cleared.")


def insert_specialties(cursor):
    """Insert specialty data"""
    print("Inserting specialties...")
    query = "INSERT INTO specialties (specialty_id, specialty_name, specialty_code) VALUES (%s, %s, %s)"
    cursor.executemany(query, SPECIALTIES)
    print(f"  Inserted {len(SPECIALTIES)} specialties")


def insert_departments(cursor):
    """Insert department data"""
    print("Inserting departments...")
    query = "INSERT INTO departments (department_id, department_name, floor, capacity) VALUES (%s, %s, %s, %s)"
    cursor.executemany(query, DEPARTMENTS)
    print(f"  Inserted {len(DEPARTMENTS)} departments")


def insert_diagnoses(cursor):
    """Insert diagnosis data"""
    print("Inserting diagnoses...")
    query = "INSERT INTO diagnoses (diagnosis_id, icd10_code, icd10_description) VALUES (%s, %s, %s)"
    cursor.executemany(query, DIAGNOSES)
    print(f"  Inserted {len(DIAGNOSES)} diagnoses")


def insert_procedures(cursor):
    """Insert procedure data"""
    print("Inserting procedures...")
    query = "INSERT INTO procedures (procedure_id, cpt_code, cpt_description) VALUES (%s, %s, %s)"
    cursor.executemany(query, PROCEDURES)
    print(f"  Inserted {len(PROCEDURES)} procedures")


def insert_patients(cursor, count=10000):
    """Insert patient data"""
    print(f"Inserting {count} patients...")
    query = """INSERT INTO patients 
               (patient_id, first_name, last_name, date_of_birth, gender, mrn) 
               VALUES (%s, %s, %s, %s, %s, %s)"""
    
    patients = []
    for i in range(1, count + 1):
        patient = (
            i,
            random.choice(FIRST_NAMES),
            random.choice(LAST_NAMES),
            random_date().strftime('%Y-%m-%d'),
            random.choice(['M', 'F']),
            f'MRN{i:06d}'
        )
        patients.append(patient)
        
        if i % 1000 == 0:
            cursor.executemany(query, patients)
            print(f"  Inserted {i} patients...")
            patients = []
    
    if patients:
        cursor.executemany(query, patients)
    
    print(f"  Total: {count} patients inserted")


def insert_providers(cursor, count=200):
    """Insert provider data"""
    print(f"Inserting {count} providers...")
    query = """INSERT INTO providers 
               (provider_id, first_name, last_name, credential, specialty_id, department_id) 
               VALUES (%s, %s, %s, %s, %s, %s)"""
    
    providers = []
    for i in range(1, count + 1):
        specialty_id = random.randint(1, len(SPECIALTIES))
        provider = (
            i,
            random.choice(FIRST_NAMES),
            random.choice(LAST_NAMES),
            random.choice(CREDENTIALS),
            specialty_id,
            specialty_id  # Same as specialty for simplicity
        )
        providers.append(provider)
    
    cursor.executemany(query, providers)
    print(f"  Inserted {count} providers")
    return count


def insert_encounters(cursor, patient_count=10000, encounter_count=50000):
    """Insert encounter data"""
    print(f"Inserting {encounter_count} encounters...")
    query = """INSERT INTO encounters 
               (encounter_id, patient_id, provider_id, encounter_type, 
                encounter_date, discharge_date, department_id) 
               VALUES (%s, %s, %s, %s, %s, %s, %s)"""
    
    encounters = []
    for i in range(1, encounter_count + 1):
        encounter_date = random_encounter_date()
        encounter_type = random.choice(ENCOUNTER_TYPES)
        
        # Discharge date depends on encounter type
        if encounter_type == 'Outpatient':
            discharge_date = encounter_date + timedelta(hours=random.randint(1, 4))
        elif encounter_type == 'Inpatient':
            discharge_date = encounter_date + timedelta(days=random.randint(1, 14))
        else:  # ER
            discharge_date = encounter_date + timedelta(hours=random.randint(2, 24))
        
        department_id = random.randint(1, len(DEPARTMENTS))
        
        encounter = (
            i,
            random.randint(1, patient_count),
            random.randint(1, 200),  # provider_id
            encounter_type,
            encounter_date.strftime('%Y-%m-%d %H:%M:%S'),
            discharge_date.strftime('%Y-%m-%d %H:%M:%S'),
            department_id
        )
        encounters.append(encounter)
        
        if i % 5000 == 0:
            cursor.executemany(query, encounters)
            print(f"  Inserted {i} encounters...")
            encounters = []
    
    if encounters:
        cursor.executemany(query, encounters)
    
    print(f"  Total: {encounter_count} encounters inserted")


def insert_encounter_diagnoses(cursor, encounter_count=50000):
    """Insert encounter-diagnosis junction data (1-4 diagnoses per encounter)"""
    print("Inserting encounter diagnoses...")
    query = """INSERT INTO encounter_diagnoses 
               (encounter_diagnosis_id, encounter_id, diagnosis_id, diagnosis_sequence) 
               VALUES (%s, %s, %s, %s)"""
    
    enc_diagnoses = []
    ed_id = 1
    
    for enc_id in range(1, encounter_count + 1):
        # Each encounter has 1-4 diagnoses
        num_diagnoses = random.randint(1, 4)
        diagnosis_ids = random.sample(range(1, len(DIAGNOSES) + 1), num_diagnoses)
        
        for seq, diag_id in enumerate(diagnosis_ids, 1):
            enc_diagnoses.append((ed_id, enc_id, diag_id, seq))
            ed_id += 1
        
        if enc_id % 5000 == 0:
            cursor.executemany(query, enc_diagnoses)
            print(f"  Processed {enc_id} encounters (diagnoses)...")
            enc_diagnoses = []
    
    if enc_diagnoses:
        cursor.executemany(query, enc_diagnoses)
    
    print(f"  Total: {ed_id - 1} encounter-diagnosis records inserted")


def insert_encounter_procedures(cursor, encounter_count=50000):
    """Insert encounter-procedure junction data (1-3 procedures per encounter)"""
    print("Inserting encounter procedures...")
    query = """INSERT INTO encounter_procedures 
               (encounter_procedure_id, encounter_id, procedure_id, procedure_date) 
               VALUES (%s, %s, %s, %s)"""
    
    enc_procedures = []
    ep_id = 1
    
    for enc_id in range(1, encounter_count + 1):
        # Each encounter has 1-3 procedures
        num_procedures = random.randint(1, 3)
        procedure_ids = random.sample(range(1, len(PROCEDURES) + 1), num_procedures)
        
        # Get a random date for procedure (within encounter timeframe)
        proc_date = random_encounter_date()
        
        for proc_id in procedure_ids:
            enc_procedures.append((ep_id, enc_id, proc_id, proc_date.strftime('%Y-%m-%d')))
            ep_id += 1
        
        if enc_id % 5000 == 0:
            cursor.executemany(query, enc_procedures)
            print(f"  Processed {enc_id} encounters (procedures)...")
            enc_procedures = []
    
    if enc_procedures:
        cursor.executemany(query, enc_procedures)
    
    print(f"  Total: {ep_id - 1} encounter-procedure records inserted")


def insert_billing(cursor, encounter_count=50000):
    """Insert billing data (1 billing record per encounter)"""
    print("Inserting billing records...")
    query = """INSERT INTO billing 
               (billing_id, encounter_id, claim_amount, allowed_amount, claim_date, claim_status) 
               VALUES (%s, %s, %s, %s, %s, %s)"""
    
    billing_records = []
    
    for i in range(1, encounter_count + 1):
        claim_amount = round(random.uniform(100, 50000), 2)
        # Allowed amount is typically 60-95% of claim amount
        allowed_amount = round(claim_amount * random.uniform(0.6, 0.95), 2)
        claim_date = random_encounter_date()
        
        billing = (
            i,
            i,  # encounter_id
            claim_amount,
            allowed_amount,
            claim_date.strftime('%Y-%m-%d'),
            random.choice(CLAIM_STATUSES)
        )
        billing_records.append(billing)
        
        if i % 5000 == 0:
            cursor.executemany(query, billing_records)
            print(f"  Inserted {i} billing records...")
            billing_records = []
    
    if billing_records:
        cursor.executemany(query, billing_records)
    
    print(f"  Total: {encounter_count} billing records inserted")


def generate_all_data():
    """Main function to generate all data"""
    print("=" * 60)
    print("Healthcare Analytics Data Generator")
    print("=" * 60)
    print()
    
    connection = get_connection()
    if not connection:
        print("Failed to connect to database. Check your DB_CONFIG settings.")
        return
    
    cursor = connection.cursor()
    
    try:
        # Clear existing data
        clear_existing_data(cursor)
        connection.commit()
        
        # Insert reference data
        insert_specialties(cursor)
        insert_departments(cursor)
        insert_diagnoses(cursor)
        insert_procedures(cursor)
        connection.commit()
        
        # Insert main data (10,000 patients, 50,000 encounters)
        insert_patients(cursor, count=10000)
        connection.commit()
        
        insert_providers(cursor, count=200)
        connection.commit()
        
        insert_encounters(cursor, patient_count=10000, encounter_count=50000)
        connection.commit()
        
        # Insert junction table data
        insert_encounter_diagnoses(cursor, encounter_count=50000)
        connection.commit()
        
        insert_encounter_procedures(cursor, encounter_count=50000)
        connection.commit()
        
        # Insert billing data
        insert_billing(cursor, encounter_count=50000)
        connection.commit()
        
        print()
        print("=" * 60)
        print("DATA GENERATION COMPLETE!")
        print("=" * 60)
        print()
        print("Summary:")
        print("  - Patients:            10,000")
        print("  - Providers:              200")
        print("  - Specialties:             10")
        print("  - Departments:             10")
        print("  - Diagnoses:               50")
        print("  - Procedures:              30")
        print("  - Encounters:          50,000")
        print("  - Encounter Diagnoses: ~125,000 (1-4 per encounter)")
        print("  - Encounter Procedures: ~100,000 (1-3 per encounter)")
        print("  - Billing Records:     50,000")
        print()
        
    except Error as e:
        print(f"Error: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()
        print("Database connection closed.")


if __name__ == "__main__":
    generate_all_data()
