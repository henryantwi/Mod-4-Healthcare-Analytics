# Healthcare Analytics Data Modeling Lab

A comprehensive data modeling project demonstrating the transformation from **OLTP (Online Transaction Processing)** to **OLAP (Online Analytical Processing)** using a Star Schema design pattern.

## Project Structure

```
DEM04-Data-Modeling/
├── config/                          # Configuration files
│   └── __init__.py                  # Database configuration settings
├── docs/                            # Project documentation & deliverables
│   ├── design_decisions.txt         # Star schema design rationale
│   ├── etl_design.txt               # ETL pipeline documentation
│   ├── query_analysis.txt           # OLTP query performance analysis
│   ├── reflection.md                # Project analysis & insights
│   ├── star_schema.sql              # Star schema DDL (deliverable copy)
│   └── star_schema_queries.txt      # Optimized OLAP queries
├── sql/                             # SQL Scripts
│   ├── oltp/                        # Operational database (3NF)
│   │   ├── schema.sql               # OLTP table definitions
│   │   └── queries.sql              # OLTP operational queries
│   └── olap/                        # Analytical database (Star Schema)
│       ├── schema.sql               # Star schema definitions
│       └── queries.sql              # Analytical queries
├── src/                             # Source code
│   ├── __init__.py
│   ├── etl/                         # ETL pipeline
│   │   ├── __init__.py
│   │   ├── load.py                  # ETL load operations
│   │   └── setup_star_schema.py     # Star schema setup script
│   └── generators/                  # Data generation
│       ├── __init__.py
│       └── generate_data.py         # Synthetic data generator
├── .env                             # Environment variables
├── .gitignore
├── docker-compose.yml               # Docker services configuration
├── pyproject.toml                   # Python project configuration
├── requirements.txt                 # Python dependencies
└── README.md
```

## Architecture

### OLTP Schema (Operational - 3NF)
- Normalized third normal form design
- Optimized for transactional operations (INSERT, UPDATE, DELETE)
- 8 Tables: `patients`, `providers`, `specialties`, `departments`, `encounters`, `diagnoses`, `procedures`, `billing`
- Junction tables: `encounter_diagnoses`, `encounter_procedures`

### Star Schema (Analytical - OLAP)
- Denormalized dimensional model
- Optimized for analytical queries and reporting
- **Fact Table**: `fact_encounters` (grain: one row per encounter)
- **Dimension Tables**: 
  - `dim_date` - Calendar dimension with pre-extracted date attributes
  - `dim_patient` - Patient demographics with derived age groups
  - `dim_provider` - Provider info with denormalized specialty/department
  - `dim_department` - Hospital department attributes
  - `dim_diagnosis` - ICD-10 diagnosis codes
  - `dim_procedure` - CPT procedure codes
  - `dim_encounter_type` - Encounter type lookup
- **Bridge Tables**: 
  - `bridge_encounter_diagnosis` - Many-to-many encounter-diagnosis
  - `bridge_encounter_procedure` - Many-to-many encounter-procedure

## Getting Started

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

## Key Features

- **10,000+ patient records** for realistic performance testing
- **50,000+ encounter records** with multi-value relationships
- **Healthcare domain modeling** with ICD-10 diagnoses and CPT procedures
- **Complete ETL pipeline** with data transformation and loading
- **Performance comparison** between OLTP and Star Schema queries
- **Pre-aggregated metrics** in fact table (diagnosis count, procedure count, billing totals)

## Deliverables

| Part | File | Description |
|------|------|-------------|
| Part 2 | `docs/query_analysis.txt` | 4 OLTP queries with performance analysis |
| Part 3 | `docs/design_decisions.txt` | Star schema design choices |
| Part 3 | `docs/star_schema.sql` | Complete star schema DDL |
| Part 3 | `docs/star_schema_queries.txt` | 4 optimized OLAP queries |
| Part 3 | `docs/etl_design.txt` | ETL pipeline documentation |
| Part 4 | `docs/reflection.md` | Performance analysis & trade-offs |

## Documentation

- `docs/design_decisions.txt` - Fact table grain, dimension design, bridge table rationale
- `docs/etl_design.txt` - Dimension load logic, fact table ETL, refresh strategies
- `docs/query_analysis.txt` - OLTP query bottlenecks and performance metrics
- `docs/reflection.md` - Star schema benefits, trade-offs, and performance quantification

## Technology Stack

- **Database**: MySQL 8.0
- **Language**: Python 3.11
- **Package Manager**: UV
- **Containerization**: Docker
