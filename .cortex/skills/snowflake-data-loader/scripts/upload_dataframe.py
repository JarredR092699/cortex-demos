#!/usr/bin/env python3
"""
Upload a pandas DataFrame directly to Snowflake using write_pandas.

Used for file formats that Snowflake cannot ingest natively via COPY INTO
(e.g., Excel .xlsx/.xls files).

Usage:
    python upload_dataframe.py <file_path> --database DB --schema SCHEMA
        --account ACCOUNT --user USER [--table TABLE] [--role ROLE] [--warehouse WH]

Requires: snowflake-connector-python[pandas], pandas, openpyxl (for .xlsx)

The script reads the file into a DataFrame using pandas, then uses
snowflake.connector.pandas_tools.write_pandas to push it to a Snowflake table.
The table is created automatically if it does not exist.
"""

import argparse
import sys
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    print("Error: pandas is required. Install with: pip install pandas", file=sys.stderr)
    sys.exit(1)

try:
    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas
except ImportError:
    print(
        "Error: snowflake-connector-python[pandas] is required.\n"
        "Install with: pip install snowflake-connector-python[pandas]",
        file=sys.stderr,
    )
    sys.exit(1)


# ---------------------------------------------------------------------------
# File readers
# ---------------------------------------------------------------------------

def _read_file(path):
    ext = path.suffix.lower()
    if ext in (".xlsx", ".xls"):
        return pd.read_excel(path)
    if ext == ".csv":
        return pd.read_csv(path)
    if ext == ".tsv":
        return pd.read_csv(path, sep="\t")
    if ext in (".json", ".jsonl", ".ndjson"):
        try:
            return pd.read_json(path, lines=True)
        except ValueError:
            return pd.read_json(path)
    if ext in (".parquet", ".pq"):
        return pd.read_parquet(path)
    raise ValueError(f"Unsupported extension: {ext}")


def _sanitize_col_name(name):
    s = str(name).strip().upper()
    s = "".join(c if c.isalnum() or c == "_" else "_" for c in s)
    if not s or s[0].isdigit():
        s = "COL_" + s
    return s


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Upload a local data file to Snowflake via write_pandas."
    )
    parser.add_argument("file", help="Path to the data file")
    parser.add_argument("--database", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--table", default=None, help="Table name (default: derived from filename)")
    parser.add_argument("--account", required=True, help="Snowflake account identifier")
    parser.add_argument("--user", required=True, help="Snowflake username")
    parser.add_argument("--role", default=None)
    parser.add_argument("--warehouse", default=None)
    parser.add_argument("--authenticator", default="externalbrowser",
                        help="Auth method (default: externalbrowser)")
    args = parser.parse_args()

    path = Path(args.file)
    if not path.exists():
        print(f"Error: File not found: {path}", file=sys.stderr)
        sys.exit(1)

    # Derive table name
    table_name = args.table
    if table_name is None:
        table_name = path.stem.upper()
        table_name = "".join(c if c.isalnum() or c == "_" else "_" for c in table_name)
        if not table_name or table_name[0].isdigit():
            table_name = "TBL_" + table_name

    # Read file
    print(f"Reading {path.name}...")
    df = _read_file(path)

    # Sanitize column names
    df.columns = [_sanitize_col_name(c) for c in df.columns]

    print(f"  {len(df)} rows, {len(df.columns)} columns")
    print(f"  Columns: {', '.join(df.columns)}")

    # Connect and upload
    conn_params = {
        "account": args.account,
        "user": args.user,
        "database": args.database,
        "schema": args.schema,
        "authenticator": args.authenticator,
    }
    if args.role:
        conn_params["role"] = args.role
    if args.warehouse:
        conn_params["warehouse"] = args.warehouse

    print(f"Connecting to {args.account}...")
    try:
        conn = snowflake.connector.connect(**conn_params)
    except Exception as e:
        print(f"Error connecting: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        print(f"Uploading to {args.database}.{args.schema}.{table_name}...")
        success, num_chunks, num_rows, output = write_pandas(
            conn,
            df,
            table_name=table_name,
            database=args.database,
            schema=args.schema,
            auto_create_table=True,
            overwrite=False,
        )
        if success:
            print(f"Done. {num_rows} rows loaded in {num_chunks} chunk(s).")
        else:
            print(f"Upload returned success=False. Output: {output}", file=sys.stderr)
            sys.exit(1)
    except Exception as e:
        print(f"Error uploading: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
