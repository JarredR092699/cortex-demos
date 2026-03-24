#!/usr/bin/env python3
"""
Schema inference for local data files.

Reads a sample from a CSV, JSON, Parquet, or Excel file and infers
Snowflake-compatible column names and types.

Usage:
    python infer_schema.py <file_path> [--sample-rows N]

Output:
    JSON array of {"name": "<COL>", "type": "<SNOWFLAKE_TYPE>"} to stdout.
"""

import argparse
import json
import sys
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    print("Error: pandas is required. Install with: pip install pandas", file=sys.stderr)
    sys.exit(1)


# ---------------------------------------------------------------------------
# Pandas dtype -> Snowflake type mapping
# ---------------------------------------------------------------------------

def _map_dtype(dtype, series=None):
    """Map a pandas dtype to a Snowflake SQL type."""
    dtype_str = str(dtype)

    if dtype_str.startswith("int"):
        return "NUMBER(38,0)"
    if dtype_str.startswith("float"):
        return "FLOAT"
    if dtype_str.startswith("bool"):
        return "BOOLEAN"
    if dtype_str.startswith("datetime"):
        return "TIMESTAMP_NTZ"
    if dtype_str == "object" and series is not None:
        # Check if it looks like dates
        try:
            pd.to_datetime(series.dropna().head(20))
            return "TIMESTAMP_NTZ"
        except (ValueError, TypeError):
            pass
        # Check max string length for VARCHAR sizing
        max_len = series.dropna().astype(str).str.len().max()
        if pd.isna(max_len):
            return "VARCHAR(256)"
        if max_len <= 64:
            return "VARCHAR(64)"
        if max_len <= 256:
            return "VARCHAR(256)"
        if max_len <= 4096:
            return "VARCHAR(4096)"
        return "VARCHAR(16777216)"
    if dtype_str == "object":
        return "VARCHAR(256)"
    return "VARCHAR(256)"


def _sanitize_col_name(name):
    """Sanitize a column name for Snowflake (uppercase, safe characters)."""
    s = str(name).strip().upper()
    s = "".join(c if c.isalnum() or c == "_" else "_" for c in s)
    if not s or s[0].isdigit():
        s = "COL_" + s
    return s


# ---------------------------------------------------------------------------
# File readers
# ---------------------------------------------------------------------------

def _read_csv(path, sample_rows):
    return pd.read_csv(path, nrows=sample_rows)


def _read_json(path, sample_rows):
    try:
        df = pd.read_json(path, lines=True, nrows=sample_rows)
    except ValueError:
        df = pd.read_json(path)
        if sample_rows:
            df = df.head(sample_rows)
    return df


def _read_parquet(path, sample_rows):
    try:
        import pyarrow.parquet as pq
        pf = pq.ParquetFile(path)
        batch = next(pf.iter_batches(batch_size=sample_rows or 1000))
        return batch.to_pandas()
    except ImportError:
        df = pd.read_parquet(path)
        if sample_rows:
            df = df.head(sample_rows)
        return df


def _read_excel(path, sample_rows):
    df = pd.read_excel(path, nrows=sample_rows)
    return df


READERS = {
    ".csv": _read_csv,
    ".tsv": _read_csv,
    ".json": _read_json,
    ".jsonl": _read_json,
    ".ndjson": _read_json,
    ".parquet": _read_parquet,
    ".pq": _read_parquet,
    ".xlsx": _read_excel,
    ".xls": _read_excel,
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def infer_schema(file_path, sample_rows=500):
    """Return a list of {name, type} dicts inferred from the file."""
    path = Path(file_path)
    ext = path.suffix.lower()

    reader = READERS.get(ext)
    if reader is None:
        raise ValueError(f"Unsupported file extension: {ext}. Supported: {', '.join(READERS)}")

    df = reader(str(path), sample_rows)

    schema = []
    for col in df.columns:
        sf_name = _sanitize_col_name(col)
        sf_type = _map_dtype(df[col].dtype, df[col])
        schema.append({"name": sf_name, "type": sf_type})

    return schema


def main():
    parser = argparse.ArgumentParser(description="Infer Snowflake schema from a local data file.")
    parser.add_argument("file", help="Path to the data file")
    parser.add_argument("--sample-rows", type=int, default=500, help="Number of rows to sample (default: 500)")
    args = parser.parse_args()

    try:
        schema = infer_schema(args.file, args.sample_rows)
        print(json.dumps(schema, indent=2))
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
