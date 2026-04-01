# Cortex Demos

A collection of Cortex Code skills demonstrating data engineering workflows with Snowflake and Coalesce.

## Skills

### Snowflake Data Loader

Scan local folders and load data files into Snowflake with automatic schema inference.

**Supported formats:** CSV / TSV, JSON / JSONL / NDJSON, Parquet, Excel (.xlsx / .xls)

**How it works:**
1. Scan a folder for supported data files
2. Infer Snowflake-compatible schema from file contents
3. Review and confirm column names/types
4. Create tables and load data automatically

```
.cortex/skills/snowflake-data-loader/
├── SKILL.md
├── scripts/
│   ├── infer_schema.py       # Schema inference from files
│   ├── generate_load_sql.py  # SQL generation (CSV, JSON, Parquet)
│   └── upload_dataframe.py   # Direct pandas upload (Excel)
└── references/
    └── type-mapping.md       # Type mapping and format reference
```

---

### Coalesce Pipeline Builder

Build data transformation pipelines in Coalesce from source tables — creating Stage, Dimension, Fact, or View nodes with SQL transforms, business keys, JOIN conditions, and column logic. Supports both single-entity pipelines and full star schemas.

**How it works:**
1. Collect requirements (workspace ID, source table, pipeline spec)
2. Orient — list workspace nodes, inspect existing patterns
3. Create Stage node(s) and apply column transforms
4. Create downstream nodes (Dimension, Fact, or View)
5. Configure business keys, JOIN conditions, and run verification

```
.cortex/skills/coalesce-pipeline-builder/
└── SKILL.md
```

---

### Coalesce Job Failure Investigation

Diagnose why a Coalesce pipeline run failed — identify the root cause, inspect failing SQL, and guide toward a fix. Can optionally apply the fix and verify with a retry run.

**How it works:**
1. Find failed runs (or use a provided run ID)
2. Investigate the failure — root cause, failing nodes, downstream impact
3. Inspect the failing node's SQL
4. Recommend (and optionally apply) a fix
5. Optionally retry the run to verify the fix

```
.cortex/skills/coalesce-job-failure-investigation/
└── SKILL.md
```

---

## Sample Data

```
data/
├── employee_hr_data_2025.xlsx
└── sales_transactions_2025.xlsx
```
