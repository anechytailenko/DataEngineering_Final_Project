# 04 Assignment — Final Project

## Objective

Design and implement a **production-style analytics project** using **dbt** and **Airflow**. The solution should demonstrate understanding of **data modeling, ELT/ETL processes, orchestration, scheduling strategies, and analytical insight generation**.

Your project must integrate multiple data source types, follow dbt best practices, and produce well-structured analytical outputs.

Students may work **individually or in groups of up to 3 people**.

---

# Requirements (30 points)

## Required data sources

Your project must use **three types of sources**:

1. **OLTP database**

   * Example: PostgreSQL, MySQL, SQLite
   * Used for structured or transactional data

2. **JSON files**

   * Example: API responses, logs, semi-structured data

3. **dbt seeds**

   * Used only for small reference datasets:

     * mappings
     * category lists
     * lookup tables

Seeds must NOT be used for large datasets.

---

## Project requirements

### 1. Minimum 20 dbt models

Create at least **20 models** organized into logical layers.

---

### 2. Layered architecture

Implement the following structure:

* **raw layer**

  * Source-aligned tables
  * minimal transformation

* **stage (stg) layer**

  * cleaned data
  * standardized naming
  * type casting

* **mart layer**

  * business-level models
  * aggregated or enriched datasets

---

### 3. Window functions

Use at least one window function, for example:

* ranking
* cumulative metrics
* lag / lead
* rolling aggregates

---

### 4. Follow dbt style guide

Apply naming and formatting standards from:

https://docs.getdbt.com/best-practices/how-we-style/1-how-we-style-our-dbt-models

---

### 5. Tests

Implement dbt tests:

* unique
* not_null
* relationships
* accepted_values (when relevant)

---

### 6. Seeds

Use seeds only for small datasets:

Examples:

* country codes
* product categories
* mapping tables

---

### 7. Data insights

Provide useful analytical outputs:

Examples:

* KPIs
* aggregated metrics
* trends
* comparisons
* business insights

Insights must be based on mart models.

---

### 8. ETL pipeline

Use **Airflow** to extract data from the OLTP database and load it into **DuckDB**.

Airflow DAG must use at least one processing framework:

* Pandas
* Polars
* Spark

Pipeline flow example:

OLTP → Python processing → DuckDB → dbt models

---

### 9. Orchestration with Airflow

Airflow must orchestrate the pipeline:

* run ETL process
* trigger dbt build command
* ensure reproducibility of the workflow

Example command:

```
dbt build
```

---

### 10. Model scheduling using tags

Models must be tagged according to refresh frequency.

Use dbt tags to separate models into execution groups:

```sql
{{ config(tags=['hourly']) }}

{{ config(tags=['daily']) }}
```

Requirements:

Create at least two tag groups:

* hourly models
* daily models

Airflow DAG must:

run hourly models every hour:

```
dbt build --select tag:hourly
```

run daily models only on scheduled dates:

```
dbt build --select tag:daily
```

This demonstrates production-style orchestration and resource optimization.

---

### 11. Documentation and explanation

Be able to clearly explain:

* architecture decisions
* modeling approach
* transformation logic
* DAG structure
* scheduling strategy
* dependencies between models

Use correct terminology:

ELT
ETL
staging layer
mart
lineage
DAG
orchestration
dependencies
tags
scheduling

---

# Additional requirement (+2.5 points)

Use **MinIO** as a source for large CSV files instead of dbt seeds.

Seeds are not designed for large datasets.

Example:

MinIO → DuckDB → dbt staging models

---

# Evaluation table (30 + 2.5 points)
| Category          | Requirement                                                                         | Points       |
| ----------------- | ----------------------------------------------------------------------------------- | ------------ |
| **Data sources**  | Uses OLTP database                                                                  | 3            |
|                   | Uses JSON files                                                                     | 2            |
|                   | Uses seeds correctly (only small datasets)                                          | 2            |
| **dbt modeling**  | Minimum 20 models implemented                                                       | 5            |
|                   | Correct architecture: raw / stage / mart                                            | 4            |
|                   | Window functions implemented                                                        | 2            |
|                   | dbt style guide followed                                                            | 2            |
| **Data quality**  | Tests implemented                                                                   | 3            |
| **Insights**      | Useful analytical outputs provided                                                  | 3            |
| **ETL pipeline**  | Airflow extracts data from OLTP → DuckDB                                            | 2            |
|                   | Uses Pandas / Polars / Spark                                                        | 1            |
| **Orchestration** | Airflow runs dbt build                                                              | 1            |
| **Scheduling**    | Tags implemented (hourly, daily)                                                    | 2            |
|                   | DAG runs hourly models hourly                                                       | 1            |
|                   | DAG runs daily models on schedule                                                   | 1            |
| **Theory**        | Correct explanation of architecture, dbt layers, orchestration, lineage, scheduling | **5**        |
|                   | **Penalty:** score may be reduced if student cannot explain their own code          | **up to -5** |
| **Additional**    | MinIO used for large CSV files                                                      | **+2.5**     |

### Notes on evaluation

#### Theory (5 points) includes understanding of:

- dbt layers (raw, stage, mart)
- DAG structure
- ETL vs ELT
- orchestration
- lineage
- scheduling strategy
- tags usage (hourly vs daily)
- data modeling decisions

#### Penalty up to -10 points may be applied if a student:

- cannot explain their code
- cannot describe transformation logic
- cannot explain DAG structure
- cannot explain why certain models are materialized
- cannot explain dependencies between models