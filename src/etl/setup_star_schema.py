"""
Healthcare Analytics Star Schema Setup
DDL execution script for creating the dimensional model and populating static dimensions.
"""

import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta

# Configuration
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


def create_star_schema_tables(cursor):
    """Create all star schema tables"""
    print("Creating star schema tables...")
    
    # Drop tables in correct order (respect foreign keys)
    drop_tables = [
        "DROP TABLE IF EXISTS bridge_encounter_procedure",
        "DROP TABLE IF EXISTS bridge_encounter_diagnosis",
        "DROP TABLE IF EXISTS fact_encounters",
        "DROP TABLE IF EXISTS dim_procedure",
        "DROP TABLE IF EXISTS dim_diagnosis",
        "DROP TABLE IF EXISTS dim_encounter_type",
        "DROP TABLE IF EXISTS dim_department",
        "DROP TABLE IF EXISTS dim_provider",
        "DROP TABLE IF EXISTS dim_patient",
        "DROP TABLE IF EXISTS dim_date"
    ]
    
    for sql in drop_tables:
        cursor.execute(sql)
    print("  - Dropped existing tables")
    
    # Create dimension tables
    cursor.execute("""
    CREATE TABLE dim_date (
        date_key INT PRIMARY KEY,
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
    )
    """)
    print("  - Created dim_date")
    
    cursor.execute("""
    CREATE TABLE dim_patient (
        patient_key INT PRIMARY KEY AUTO_INCREMENT,
        patient_id INT NOT NULL,
        first_name VARCHAR(100),
        last_name VARCHAR(100),
        full_name VARCHAR(200),
        date_of_birth DATE,
        gender CHAR(1),
        gender_description VARCHAR(10),
        age INT,
        age_group VARCHAR(20),
        mrn VARCHAR(20),
        effective_date DATE DEFAULT (CURRENT_DATE),
        UNIQUE INDEX idx_patient_id (patient_id),
        INDEX idx_age_group (age_group),
        INDEX idx_gender (gender)
    )
    """)
    print("  - Created dim_patient")
    
    cursor.execute("""
    CREATE TABLE dim_provider (
        provider_key INT PRIMARY KEY AUTO_INCREMENT,
        provider_id INT NOT NULL,
        first_name VARCHAR(100),
        last_name VARCHAR(100),
        full_name VARCHAR(200),
        credential VARCHAR(20),
        specialty_id INT,
        specialty_name VARCHAR(100),
        specialty_code VARCHAR(10),
        department_id INT,
        department_name VARCHAR(100),
        UNIQUE INDEX idx_provider_id (provider_id),
        INDEX idx_specialty (specialty_name),
        INDEX idx_department (department_name)
    )
    """)
    print("  - Created dim_provider")
    
    cursor.execute("""
    CREATE TABLE dim_department (
        department_key INT PRIMARY KEY AUTO_INCREMENT,
        department_id INT NOT NULL,
        department_name VARCHAR(100),
        floor INT,
        capacity INT,
        UNIQUE INDEX idx_department_id (department_id)
    )
    """)
    print("  - Created dim_department")
    
    cursor.execute("""
    CREATE TABLE dim_encounter_type (
        encounter_type_key INT PRIMARY KEY AUTO_INCREMENT,
        encounter_type_name VARCHAR(50) NOT NULL,
        encounter_type_category VARCHAR(50),
        UNIQUE INDEX idx_encounter_type_name (encounter_type_name)
    )
    """)
    print("  - Created dim_encounter_type")
    
    cursor.execute("""
    CREATE TABLE dim_diagnosis (
        diagnosis_key INT PRIMARY KEY AUTO_INCREMENT,
        diagnosis_id INT NOT NULL,
        icd10_code VARCHAR(10) NOT NULL,
        icd10_description VARCHAR(200),
        diagnosis_category VARCHAR(100),
        UNIQUE INDEX idx_diagnosis_id (diagnosis_id),
        INDEX idx_icd10_code (icd10_code)
    )
    """)
    print("  - Created dim_diagnosis")
    
    cursor.execute("""
    CREATE TABLE dim_procedure (
        procedure_key INT PRIMARY KEY AUTO_INCREMENT,
        procedure_id INT NOT NULL,
        cpt_code VARCHAR(10) NOT NULL,
        cpt_description VARCHAR(200),
        procedure_category VARCHAR(100),
        UNIQUE INDEX idx_procedure_id (procedure_id),
        INDEX idx_cpt_code (cpt_code)
    )
    """)
    print("  - Created dim_procedure")
    
    cursor.execute("""
    CREATE TABLE fact_encounters (
        encounter_key INT PRIMARY KEY AUTO_INCREMENT,
        encounter_id INT NOT NULL,
        date_key INT NOT NULL,
        discharge_date_key INT,
        patient_key INT NOT NULL,
        provider_key INT NOT NULL,
        department_key INT NOT NULL,
        encounter_type_key INT NOT NULL,
        encounter_date DATETIME,
        discharge_date DATETIME,
        diagnosis_count INT DEFAULT 0,
        procedure_count INT DEFAULT 0,
        total_claim_amount DECIMAL(12,2) DEFAULT 0,
        total_allowed_amount DECIMAL(12,2) DEFAULT 0,
        claim_status VARCHAR(50),
        length_of_stay_days INT,
        FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
        FOREIGN KEY (discharge_date_key) REFERENCES dim_date(date_key),
        FOREIGN KEY (patient_key) REFERENCES dim_patient(patient_key),
        FOREIGN KEY (provider_key) REFERENCES dim_provider(provider_key),
        FOREIGN KEY (department_key) REFERENCES dim_department(department_key),
        FOREIGN KEY (encounter_type_key) REFERENCES dim_encounter_type(encounter_type_key),
        UNIQUE INDEX idx_encounter_id (encounter_id),
        INDEX idx_date_key (date_key),
        INDEX idx_patient_key (patient_key),
        INDEX idx_provider_key (provider_key),
        INDEX idx_encounter_type_key (encounter_type_key),
        INDEX idx_date_specialty (date_key, provider_key)
    )
    """)
    print("  - Created fact_encounters")
    
    cursor.execute("""
    CREATE TABLE bridge_encounter_diagnosis (
        bridge_id INT PRIMARY KEY AUTO_INCREMENT,
        encounter_key INT NOT NULL,
        diagnosis_key INT NOT NULL,
        diagnosis_sequence INT,
        FOREIGN KEY (encounter_key) REFERENCES fact_encounters(encounter_key),
        FOREIGN KEY (diagnosis_key) REFERENCES dim_diagnosis(diagnosis_key),
        INDEX idx_encounter_key (encounter_key),
        INDEX idx_diagnosis_key (diagnosis_key),
        UNIQUE INDEX idx_enc_diag (encounter_key, diagnosis_key)
    )
    """)
    print("  - Created bridge_encounter_diagnosis")
    
    cursor.execute("""
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
    )
    """)
    print("  - Created bridge_encounter_procedure")
    
    print("All star schema tables created successfully!")


def populate_dim_date(cursor):
    """Populate the date dimension with dates from 2023-2025"""
    print("Populating dim_date...")
    
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2025, 12, 31)
    current_date = start_date
    
    day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    month_names = ['', 'January', 'February', 'March', 'April', 'May', 'June',
                   'July', 'August', 'September', 'October', 'November', 'December']
    
    dates = []
    while current_date <= end_date:
        date_key = int(current_date.strftime('%Y%m%d'))
        year = current_date.year
        quarter = (current_date.month - 1) // 3 + 1
        month = current_date.month
        month_name = month_names[month]
        week_of_year = current_date.isocalendar()[1]
        day_of_month = current_date.day
        day_of_week = current_date.weekday() + 1  # 1=Monday, 7=Sunday
        day_name = day_names[current_date.weekday()]
        is_weekend = day_of_week in (6, 7)
        
        dates.append((
            date_key, current_date.date(), year, quarter, month, month_name,
            week_of_year, day_of_month, day_of_week, day_name, is_weekend,
            year, quarter
        ))
        
        current_date += timedelta(days=1)
    
    insert_query = """
    INSERT INTO dim_date (date_key, calendar_date, year, quarter, month, month_name,
                         week_of_year, day_of_month, day_of_week, day_name, is_weekend,
                         fiscal_year, fiscal_quarter)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    cursor.executemany(insert_query, dates)
    print(f"  Loaded {len(dates)} dates into dim_date")


def populate_dim_encounter_type(cursor):
    """Populate encounter type dimension"""
    print("Populating dim_encounter_type...")
    
    cursor.execute("""
    INSERT INTO dim_encounter_type (encounter_type_name, encounter_type_category) VALUES
        ('Outpatient', 'Ambulatory'),
        ('Inpatient', 'Acute Care'),
        ('ER', 'Emergency')
    """)
    print("  Loaded 3 encounter types")


def setup_star_schema():
    """Main function to set up the star schema"""
    print("=" * 60)
    print("Star Schema Initialization")
    print("=" * 60)
    print(f"Started at: {datetime.now()}")
    print()
    
    connection = get_connection()
    if not connection:
        print("Failed to connect to database.")
        return
    
    cursor = connection.cursor()
    
    try:
        # Create tables
        create_star_schema_tables(cursor)
        connection.commit()
        
        # Populate static dimensions
        print()
        populate_dim_date(cursor)
        connection.commit()
        
        populate_dim_encounter_type(cursor)
        connection.commit()
        
        print()
        print("=" * 60)
        print("STAR SCHEMA SETUP COMPLETE!")
        print("=" * 60)
        print()
        print("Schema setup completed. Ready for ETL load process.")
        print()
        
    except Error as e:
        print(f"Error: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    setup_star_schema()
