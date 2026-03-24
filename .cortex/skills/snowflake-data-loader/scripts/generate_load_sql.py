#!/usr/bin/env python3
"""
Generate Snowflake SQL for table creation and data loading from a local file.

Usage:
    python generate_load_sql.py <file_path> --database DB --schema SCHEMA [--table TABLE] [--schema-json FILE]

Output:
    Prints the SQL statements to stdout (CREATE TABLE, CREATE STAGE, PUT, COPY INTO).
"""

import argparse
import json
import sys
from pathlib import Path


# ---------------------------------------------------------------------------
# Format detection
# ---------------------------------------------------------------------------

FORMAT_MAP = {
    ".csv": "CSV",
    ".tsv": "CSV",
    ".json": "JSON",
    ".jsonl": "JSON",
    ".ndjson": "JSON",
    ".parquet": "PARQUET",
    ".pq": "PARQUET",
    # Excel (.xlsx, .xls) not supported here - use upload_dataframe.py instead
}


def _file_format_options(ext, file_format):
    """Return Snowflake FILE_FORMAT options string for COPY INTO."""
    if file_format == "CSV":
        opts = "TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' SKIP_HEADER = 1"
        if ext == ".tsv":
            opts += " FIELD_DELIMITER = '\\t'"
        return opts
    if file_format == "JSON":
        return "TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE"
    if file_format == "PARQUET":
        return "TYPE = 'PARQUET'"
    return f"TYPE = '{file_format}'"


# ---------------------------------------------------------------------------
# SQL generation
# ---------------------------------------------------------------------------

def generate_sql(file_path, database, schema, table_name, col_schema, stage_name=None):
    """Generate the full set of SQL statements for loading a file."""
    path = Path(file_path)
    ext = path.suffix.lower()
    file_format = FORMAT_MAP.get(ext)

    if file_format is None:
        raise ValueError(f"Unsupported file extension: {ext}")

    if table_name is None:
        table_name = path.stem.upper()
        table_name = "".join(c if c.isalnum() or c == "_" else "_" for c in table_name)
        if not table_name or table_name[0].isdigit():
            table_name = "TBL_" + table_name

    if stage_name is None:
        stage_name = f"STG_{table_name}_LOAD"

    fq_table = f"{database}.{schema}.{table_name}"
    fq_stage = f"{database}.{schema}.{stage_name}"

    # Normalize local path for PUT (forward slashes required)
    local_path = str(path.resolve()).replace("\\", "/")

    stmts = []

    # 1. CREATE TABLE
    if file_format == "JSON" and not col_schema:
        # If no schema provided for JSON, use a single VARIANT column
        stmts.append(
            f"CREATE TABLE IF NOT EXISTS {fq_table} (\n"
            f"    RAW_DATA VARIANT\n"
            f");"
        )
    else:
        col_defs = ",\n".join(f"    {c['name']} {c['type']}" for c in col_schema)
        stmts.append(
            f"CREATE TABLE IF NOT EXISTS {fq_table} (\n"
            f"{col_defs}\n"
            f");"
        )

    # 2. CREATE TEMPORARY STAGE
    stmts.append(f"CREATE OR REPLACE TEMPORARY STAGE {fq_stage};")

    # 3. PUT
    stmts.append(f"PUT 'file://{local_path}' @{fq_stage} AUTO_COMPRESS=TRUE;")

    # 4. COPY INTO
    fmt_opts = _file_format_options(ext, file_format)

    if file_format == "PARQUET":
        # For Parquet, use MATCH_BY_COLUMN_NAME
        stmts.append(
            f"COPY INTO {fq_table}\n"
            f"    FROM @{fq_stage}\n"
            f"    FILE_FORMAT = ({fmt_opts})\n"
            f"    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;"
        )
    elif file_format == "JSON" and not col_schema:
        # Load raw JSON into VARIANT
        stmts.append(
            f"COPY INTO {fq_table}\n"
            f"    FROM (\n"
            f"        SELECT $1::VARIANT AS RAW_DATA\n"
            f"        FROM @{fq_stage}\n"
            f"    )\n"
            f"    FILE_FORMAT = ({fmt_opts});"
        )
    else:
        stmts.append(
            f"COPY INTO {fq_table}\n"
            f"    FROM @{fq_stage}\n"
            f"    FILE_FORMAT = ({fmt_opts})\n"
            f"    ON_ERROR = 'CONTINUE';"
        )

    return stmts, fq_table


def main():
    parser = argparse.ArgumentParser(description="Generate Snowflake load SQL for a local file.")
    parser.add_argument("file", help="Path to the data file")
    parser.add_argument("--database", required=True, help="Target Snowflake database")
    parser.add_argument("--schema", required=True, help="Target Snowflake schema")
    parser.add_argument("--table", default=None, help="Target table name (default: derived from filename)")
    parser.add_argument("--schema-json", default=None, help="Path to JSON schema file from infer_schema.py")
    args = parser.parse_args()

    col_schema = []
    if args.schema_json:
        with open(args.schema_json, "r") as f:
            col_schema = json.load(f)

    try:
        stmts, fq_table = generate_sql(
            args.file, args.database, args.schema, args.table, col_schema
        )
        print(f"-- Load {Path(args.file).name} into {fq_table}")
        print(f"-- Generated by snowflake-data-loader skill\n")
        for stmt in stmts:
            print(stmt)
            print()
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
