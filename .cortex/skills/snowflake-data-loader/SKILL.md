---
name: snowflake-data-loader
description: Scan local folders and load data files (CSV, JSON, Parquet, Excel) into Snowflake. Auto-infers schema from file contents, creates tables, and loads data via PUT/COPY INTO. Use when the user wants to (1) load local files into Snowflake, (2) scan a folder for data files to ingest, (3) create Snowflake tables from local data, (4) bulk import mixed-format data files, (5) monitor a directory and push its contents to Snowflake, or (6) quickly stand up tables from CSV/JSON/Parquet/Excel files on disk.
---

# Snowflake Data Loader

Load local data files into Snowflake with automatic schema inference.

## Supported Formats

CSV, TSV, JSON, JSONL/NDJSON, Parquet, Excel (.xlsx/.xls)

## Workflow

### 1. Scan the folder

Glob the user-specified folder for supported extensions:
```
*.csv, *.tsv, *.json, *.jsonl, *.ndjson, *.parquet, *.pq, *.xlsx, *.xls
```

List all discovered files with their sizes and formats. If the folder is empty or has no supported files, inform the user.

### 2. Confirm target location

Ask the user for the target `DATABASE.SCHEMA` in Snowflake. Each file becomes a table; the table name defaults to the filename (uppercased, sanitized).

### 3. Process each file

For each file, follow this sequence:

**a) Infer schema**

Run the inference script:
```
python scripts/infer_schema.py "<file_path>" --sample-rows 500
```

This outputs a JSON array of `{"name": "COL", "type": "SNOWFLAKE_TYPE"}`.

**b) Review schema with the user**

Present the inferred schema as a table. Ask the user to confirm or adjust column names/types before proceeding. If the user requests changes, modify the schema accordingly.

**c) Load the data**

Choose the loading method based on file type:

**Excel files (.xlsx, .xls)** - Use `upload_dataframe.py` (direct pandas upload):
```
python scripts/upload_dataframe.py "<file_path>" --database DB --schema SCHEMA --table TABLE \
    --account ACCOUNT --user USER [--role ROLE] [--warehouse WH]
```
This reads the file with pandas and uses `write_pandas()` from snowflake-connector-python to push the DataFrame directly. The table is auto-created. Use `cortex source <connection> --map account=SF_ACCOUNT --map user=SF_USER` to inject credentials securely.

**CSV, TSV, JSON, Parquet** - Use `generate_load_sql.py` (PUT/COPY INTO):
```
python scripts/generate_load_sql.py "<file_path>" --database DB --schema SCHEMA --table TABLE --schema-json <path_to_schema_json>
```

This generates SQL statements:
1. `CREATE TABLE IF NOT EXISTS` with the inferred columns
2. `CREATE OR REPLACE TEMPORARY STAGE` for upload
3. `PUT file:///... @stage` to upload the file
4. `COPY INTO` with format-specific options

Run each SQL statement via `snowflake_sql_execute`. Handle errors and report results.

### 4. Verify

After loading, run `SELECT COUNT(*) FROM <table>` on each new table and report row counts to the user.

## Important Notes

- **Excel files**: Uploaded directly via `write_pandas()` using `upload_dataframe.py`. Requires `snowflake-connector-python[pandas]` and `openpyxl`.
- **Nested JSON**: If JSON has nested objects, the script creates a single VARIANT column (`RAW_DATA`). Advise the user to use FLATTEN queries afterward.
- **Parquet**: Uses `MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE` for column alignment.
- **Column sanitization**: Spaces and special characters become underscores; names are uppercased; digit-leading names get a `COL_` prefix.
- **Type mapping details**: See `references/type-mapping.md` for the complete pandas-to-Snowflake type mapping and format-specific COPY INTO options.
- **Large files**: The PUT command auto-compresses files. For very large files (>1GB), warn the user that upload time may be significant.
- **Windows paths**: The PUT command requires forward slashes in file paths. The `generate_load_sql.py` script handles this automatically.
