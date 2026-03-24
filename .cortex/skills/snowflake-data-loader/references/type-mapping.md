# Type Mapping and Format Reference

## Pandas dtype to Snowflake Type

| Pandas dtype     | Snowflake Type      | Notes                                    |
|------------------|---------------------|------------------------------------------|
| int8-int64       | NUMBER(38,0)        |                                          |
| float16-float64  | FLOAT               |                                          |
| bool             | BOOLEAN             |                                          |
| datetime64       | TIMESTAMP_NTZ       |                                          |
| object (date)    | TIMESTAMP_NTZ       | Detected via `pd.to_datetime` on sample  |
| object (short)   | VARCHAR(64)         | Max length <= 64                         |
| object (medium)  | VARCHAR(256)        | Max length <= 256                        |
| object (long)    | VARCHAR(4096)       | Max length <= 4096                       |
| object (xlarge)  | VARCHAR(16777216)   | Max length > 4096                        |

## Format-Specific COPY INTO Options

### CSV
```sql
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
```
- TSV files add `FIELD_DELIMITER = '\t'`

### JSON
```sql
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE)
```
- Flat JSON: columns inferred from top-level keys
- Nested JSON: load as single VARIANT column (RAW_DATA), flatten later

### Parquet
```sql
FILE_FORMAT = (TYPE = 'PARQUET')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
```
- Column matching is automatic; schema must align with table DDL

### Excel
- Excel files (.xlsx, .xls) are read by pandas and uploaded via `write_pandas()`
- Uses `upload_dataframe.py` instead of PUT/COPY INTO
- Requires `snowflake-connector-python[pandas]` and `openpyxl`
- `write_pandas()` auto-creates the table with `auto_create_table=True`

## Edge Cases

- **Columns with spaces/special chars**: sanitized to uppercase with underscores
- **Columns starting with digits**: prefixed with `COL_`
- **Empty columns**: default to VARCHAR(256)
- **Mixed-type columns**: treated as VARCHAR, sized by max observed length
- **Nested JSON objects**: use VARIANT column and post-load FLATTEN
