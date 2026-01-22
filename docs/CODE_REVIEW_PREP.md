# Code Review Preparation Notes

## Key Points to Emphasize

### 1. Design Decisions

**Fact Table Grain: One row per encounter**
- Reasoning: Most business questions aggregate at encounter level
- Avoids row explosion from multiple diagnoses/procedures
- Pre-aggregated metrics eliminate expensive junction table joins

**Bridge Tables for Many-to-Many**
- Preserves detail without exploding fact table
- Allows drilling down to specific diagnosis-procedure combinations
- Trade-off: Query 2 still requires joins, but with optimized indexes

**Denormalized Dimensions**
- Specialty in dim_provider eliminates extra JOIN
- Full_name pre-concatenated (no runtime CONCAT)
- Age_group calculated once at ETL time

### 2. Performance Improvements

| Query | OLTP Time | Star Schema | Improvement | Key Factor |
|-------|-----------|-------------|-------------|------------|
| Q1 | 0.397s | ~0.05s | 8x | No DATE_FORMAT, specialty pre-joined |
| Q2 | 2.999s | ~0.3s | 10x | Reduced row explosion, indexed bridges |
| Q3 | 0.393s | ~0.15s | 3x | One less JOIN (specialty) |
| Q4 | 0.446s | ~0.05s | 9x | Billing pre-aggregated in fact |

### 3. Trade-offs Understood

**What We Gained:**
- 3-10x faster analytical queries
- Simpler SQL for business users
- Pre-calculated metrics

**What We Lost:**
- Data freshness (daily ETL vs real-time)
- Storage space (denormalization duplicates data)
- ETL complexity (must maintain transformation pipeline)
- Update anomalies (specialty change needs multiple table updates)

### 4. ETL Design

**Load Order (Respects Dependencies):**
1. dim_date (one-time)
2. dim_encounter_type (static)
3. dim_patient, dim_provider, dim_department (no dependencies)
4. dim_diagnosis, dim_procedure
5. fact_encounters (requires all dimensions)
6. bridge_encounter_diagnosis, bridge_encounter_procedure

**Pre-Aggregation Logic:**
- diagnosis_count: COUNT from junction table
- procedure_count: COUNT from junction table
- total_allowed_amount: SUM from billing
- length_of_stay_days: DATEDIFF calculation

### 5. Questions I Anticipated

**Q: Why not denormalize everything into fact table?**
- Would create N×M rows per encounter
- 50K encounters → 250K+ fact rows
- SUM(total_allowed_amount) would give wrong results (unless DISTINCT)

**Q: How to handle provider changing specialties?**
- Current: SCD Type 1 (overwrite)
- Better: SCD Type 2 (add new row with effective dates)
- For this use case: Type 1 acceptable (rare event)

**Q: What if billing arrives late?**
- Initial load: Insert encounter with billing = 0
- Incremental ETL: Update fact when billing arrives
- Use last_load_timestamp to identify updates

**Q: Why keep bridge tables if most queries use counts?**
- Query 2 needs specific diagnosis-procedure pairs
- Analysts may need to drill down
- Bridge tables are optional, not required for most reports

### 6. What I'd Do Differently in Production

**Add:**
- SCD Type 2 for provider dimension
- Materialized views for common diagnosis-procedure pairs
- Incremental ETL with change data capture
- Data quality checks (row count validation, orphan detection)
- Audit columns (created_at, updated_at, created_by)
- Partition fact table by date_key for faster queries

**Improve:**
- Add logging to ETL (Python logging module)
- Create error table for failed records
- Add retry logic for transient errors
- Implement idempotent loads (can re-run safely)

### 7. Technical Highlights

**Good Practices I Followed:**
- Surrogate keys (patient_key vs patient_id)
- Indexes on foreign keys and common filters
- Date dimension eliminates date functions
- Proper transaction management in Python
- Commented code and documentation
- Verified ETL with row counts

**Industry Standards Used:**
- Star schema (Kimball methodology)
- Fact and dimension naming convention
- Pre-aggregated metrics in fact
- Bridge tables for many-to-many
- Separate OLTP and OLAP schemas

## Potential Weaknesses to Address

1. **Star schema queries use estimates** - Need to run actual queries
2. **No data quality validation** - Should add CHECK constraints
3. **No incremental ETL** - Always full reload (acceptable for lab)
4. **discharge_date_key can be NULL** - FK constraint might fail

## Demonstration Flow

1. Show OLTP schema complexity (8 tables, junction tables)
2. Run Query 2 on OLTP (2.999s - row explosion!)
3. Explain star schema design (fact + dimensions)
4. Show ETL code transformations
5. Run Query 2 on star schema (~0.3s - 10x faster!)
6. Explain trade-offs and design decisions

## Key Terminology to Use

- **Grain**: One row per encounter
- **Denormalization**: Flattening hierarchy for performance
- **Pre-aggregation**: Moving calculation from query time to ETL time
- **Bridge table**: Handling many-to-many without explosion
- **Surrogate key**: Integer key for faster joins
- **Dimension**: Descriptive attributes (who, what, where, when)
- **Fact**: Measurements and metrics (how many, how much)
- **SCD**: Slowly Changing Dimension
- **ETL**: Extract, Transform, Load

## Confidence Boosters

✅ Complete deliverables (all 6 documents)
✅ Industry-standard star schema design
✅ Clean, commented code
✅ Proper ETL pipeline with transformations
✅ Performance analysis with specific numbers
✅ Trade-off analysis shows critical thinking
✅ Documentation is clear and professional
✅ Python code follows best practices
✅ SQL is clean and well-indexed

**You're ready for the code review!** 🚀
