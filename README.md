# Cortex Code Skills

A collection of custom [Cortex Code](https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code) skills for working with Snowflake and Coalesce.

## Skills

| Skill | Description |
|-------|-------------|
| [snowflake-data-loader](.cortex/skills/snowflake-data-loader/) | Scan local folders and load data files (CSV, JSON, Parquet, Excel) into Snowflake with automatic schema inference. |
| [coalesce-pipeline-builder](.cortex/skills/coalesce-pipeline-builder/) | Build data transformation pipelines in Coalesce — Stage, Dimension, Fact, and View nodes with SQL transforms, business keys, and star schema support. |
| [coalesce-job-failure-investigation](.cortex/skills/coalesce-job-failure-investigation/) | Diagnose Coalesce pipeline run failures — identify root cause, inspect failing SQL, apply fixes, and verify with a retry run. |

## Getting Started

1. Clone this repo into your working directory.
2. Open [Cortex Code](https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code) in the same directory. Skills in `.cortex/skills/` are automatically detected.
3. Ask Cortex Code to perform a task that matches a skill's description, and it will invoke the skill.

Each skill has its own `SKILL.md` with detailed workflow documentation and usage instructions.

## Sample Data

The `data/` directory contains sample datasets for testing skills:

| File | Description |
|------|-------------|
| `employee_hr_data_2025.xlsx` | 200 rows of synthetic employee HR data (names, departments, salaries, etc.) |
| `sales_transactions_2025.xlsx` | 500 rows of synthetic sales transaction data (products, quantities, revenue, etc.) |

These are synthetic datasets with no real PII. They were loaded into `SAMPLE_DATA.LOAD` as `EMPLOYEE_HR_DATA_25` and `SALES_TRANSACTIONS_25` using the snowflake-data-loader skill.
