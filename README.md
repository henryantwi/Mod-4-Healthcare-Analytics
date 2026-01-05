# Healthcare Analytics Data Modeling Lab

A comprehensive data modeling project demonstrating the transformation from **OLTP (Online Transaction Processing)** to **OLAP (Online Analytical Processing)** using a Star Schema design pattern.

## 📁 Project Structure

```
DEM04-Data-Modeling/
├── config/                     # Configuration files
│   └── __init__.py            # Database configuration settings
├── data/                       # Persistent data storage
│   └── mysql_data/            # MySQL database files (Docker volume)
├── docs/                       # Documentation
│   ├── design_decisions.txt   # Design rationale and decisions
│   ├── etl_design.txt         # ETL architecture documentation
│   ├── query_analysis.txt     # Query performance analysis
│   └── reflection.md          # Project reflections
├── src/                        # Source code
│   ├── __init__.py
│   ├── etl/                   # ETL pipeline
│   │   ├── __init__.py
│   │   ├── load.py            # ETL load operations
│   │   └── setup_star_schema.py  # Star schema setup
│   └── generators/            # Data generation
│       ├── __init__.py
│       └── generate_data.py   # Synthetic data generator
├── sql/                        # SQL Scripts
│   ├── oltp/                  # Operational database
│   │   ├── schema.sql         # OLTP table definitions
│   │   └── queries.sql        # OLTP operational queries
│   └── olap/                  # Analytical database
│       ├── schema.sql         # Star schema definitions
│       └── queries.sql        # Analytical queries
├── .env                        # Environment variables
├── .gitignore
├── docker-compose.yml          # Docker services configuration
├── pyproject.toml              # Python project configuration
└── uv.lock                     # Dependency lock file
```

## 🏗️ Architecture

### OLTP Schema (Operational)
- Normalized 3NF design
- Optimized for transactional operations
- Tables: Patients, Providers, Encounters, Diagnoses, Procedures, Billing

### Star Schema (Analytical)
- Denormalized dimensional model
- Optimized for analytical queries and reporting
- **Fact Table**: `fact_encounters`
- **Dimension Tables**: `dim_patient`, `dim_provider`, `dim_department`, `dim_diagnosis`, `dim_procedure`, `dim_date`, `dim_encounter_type`
- **Bridge Tables**: `bridge_encounter_diagnosis`, `bridge_encounter_procedure`

## 🚀 Getting Started

### Prerequisites
- Docker & Docker Compose
- Python 3.11+
- UV (Python package manager)

### Setup

1. **Start the database**:
   ```bash
   docker-compose up -d
   ```

2. **Install dependencies**:
   ```bash
   uv sync
   ```

3. **Generate sample data**:
   ```bash
   python -m src.generators.generate_data
   ```

4. **Setup Star Schema**:
   ```bash
   python -m src.etl.setup_star_schema
   ```

5. **Run ETL Process**:
   ```bash
   python -m src.etl.load
   ```

## 📊 Key Features

- **10,000+ patient records** for realistic performance testing
- **50,000+ encounter records** with multi-value relationships
- **Healthcare domain modeling** with ICD-10 diagnoses and CPT procedures
- **Complete ETL pipeline** with data transformation and loading
- **Analytical queries** for business intelligence scenarios

## 📚 Documentation

- See `docs/design_decisions.txt` for architectural decisions
- See `docs/etl_design.txt` for ETL pipeline documentation
- See `docs/query_analysis.txt` for performance benchmarks
- See `docs/reflection.md` for project insights

## 🛠️ Technology Stack

- **Database**: MySQL 8.0
- **Language**: Python 3.11
- **Package Manager**: UV
- **Containerization**: Docker
