# Cortex Demos

## Snowflake Data Loader Skill

A Cortex Code skill that scans local folders and loads data files into Snowflake with automatic schema inference.

### Supported Formats

- **CSV / TSV** - Loaded via PUT + COPY INTO
- **JSON / JSONL / NDJSON** - Loaded via PUT + COPY INTO (nested JSON stored as VARIANT)
- **Parquet** - Loaded via PUT + COPY INTO with MATCH_BY_COLUMN_NAME
- **Excel (.xlsx / .xls)** - Loaded via pandas `write_pandas()` direct upload

### How It Works

1. Scan a folder for supported data files
2. Infer Snowflake-compatible schema from file contents
3. Review and confirm column names/types
4. Create tables and load data automatically

### Skill Location

```
.snowflake/cortex/skills/snowflake-data-loader/
├── SKILL.md              # Skill instructions and workflow
├── scripts/
│   ├── infer_schema.py       # Schema inference from files
│   ├── generate_load_sql.py  # SQL generation (CSV, JSON, Parquet)
│   └── upload_dataframe.py   # Direct pandas upload (Excel)
└── references/
    └── type-mapping.md       # Type mapping and format reference
```
