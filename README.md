#  Apex Retail Intelligence
### End-to-End Data Engineering Pipeline | Medallion Architecture on Databricks

[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white)](#)
[![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apachespark&logoColor=white)](#)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD8?style=flat&logo=delta&logoColor=white)](#)
[![Unity Catalog](https://img.shields.io/badge/Unity%20Catalog-1B3A57?style=flat)](#)
[![Status](https://img.shields.io/badge/Status-Complete-brightgreen?style=flat)](#)

> A fully automated, fault-tolerant, idempotent, and auditable data pipeline — transforming raw, messy retail CSVs into a business-ready star schema and five validated KPIs, built entirely within Databricks using PySpark and Delta Lake.

---

##  Overview

**Apex Retail**, a fast-growing retail and e-commerce company, generates a continuous stream of raw, unstructured data — customer profiles, product catalogs, and sales transactions — unfit for direct business reporting.

This project implements the industry-standard **Medallion Architecture**, progressively refining that raw data through five layers — **Raw → Landing → Bronze → Silver → Gold** — culminating in a fully modeled star schema and five business-critical KPIs, all computed and rendered natively within Databricks notebooks, with **zero external BI tools**.

| Property | Detail |
|---|---|
| **Domain** | Data Engineering / Big Data |
| **Tech Stack** | Apache Spark (PySpark), Databricks, Delta Lake, Unity Catalog |
| **Architecture** | Medallion (Bronze → Silver → Gold) |
| **Author** | Lakshita — Data Engineering Intern, Celebal Technologies |
| **Programme** | CEI'26 Internship, Major Project |

---

##  Architecture

```
┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
│   RAW    │ ──▶ │ LANDING  │ ──▶ │  BRONZE  │ ──▶ │  SILVER  │ ──▶ │   GOLD   │ ──▶ │   KPIs   │
│   CSV    │     │ Parquet  │     │  Delta   │     │  Clean+  │     │   Star   │     │ Business │
│          │     │ + Audit  │     │  + Meta  │     │  Merge   │     │  Schema  │     │ Reports  │
└──────────┘     └──────────┘     └──────────┘     └──────────┘     └──────────┘     └──────────┘
```

| Layer | What Happens |
|---|---|
|  **Raw** | Source CSVs organized into `raw/`, split by entity & load type, all columns as string |
|  **Landing** | CSV → Parquet conversion, validated against `audit_landing` files (dynamic PASS/FAIL) |
|  **Bronze** | Parquet → Delta Lake, `ingested_at` metadata injected, historical/incremental kept separate |
|  **Silver** | DQ rules, Delta MERGE, SCD Type 1 & 2, surrogate keys, validated against `audit_silver` files |
|  **Gold** | Star schema (4 dimensions + 1 fact table), registered in Unity Catalog under `GOLD_tables` |
|  **KPIs** | 5 business KPIs computed inline via PySpark — no external dashboards |

---

##  Source Data

Three core datasets, each delivered as a **Historical** (bulk) and **Incremental** (daily delta) load:

| Dataset | Fields | Historical | Incremental |
|---|---|---:|---:|
| **Customer** | customer_id, age, gender, income_bracket, loyalty_program, churned, etc. | 1,052 | 1,053 |
| **Product** | product_id, product_name, category, rating, return_rate, unit_price, etc. | 1,043 | 1,041 |
| **Sales** | transaction_id, customer_id, product_id, quantity, discount, promotion, etc. | 1,002 | 1,000 |

---

##  Silver Layer — Change Management Strategy

Each entity is handled according to how it genuinely changes in a real retail system:

| Entity | Strategy | Behavior |
|---|---|---|
|  **Product** | **SCD Type 1** | Overwrite in place — no history retained |
|  **Customer** | **SCD Type 2** | Old row closed out (`is_current = false`), new active row inserted — full history preserved via `effective_start_date` / `effective_end_date` |
|  **Sales** | **Immutable Ledger** | Insert-only, never updated — deduplicated via window functions before merge |

All incremental processing uses **Delta MERGE semantics** — no watermarking, per project constraints.

---

##  Gold Layer — Star Schema

```
                              ┌───────────────┐
                              │  dim_customer │
                              └───────┬───────┘
                                      │
      ┌───────────────┐      ┌───────▼───────┐      ┌────────────────┐
      │  dim_product   │──────│  fact_sales   │──────│  dim_promotion │
      └───────────────┘      └───────┬───────┘      └────────────────┘
                                      │
                              ┌───────▼───────┐
                              │   dim_date    │
                              └───────────────┘
```

| Table | Type | Contents |
|---|---|---|
| `dim_customer` | Dimension | Customer attributes + full SCD Type 2 history |
| `dim_product` | Dimension | Product catalogue (current state) |
| `dim_promotion` | Dimension | Distinct promotion types (extracted from sales) |
| `dim_date` | Dimension | Freshly-derived calendar attributes |
| `fact_sales` | **Fact** | Transaction metrics + surrogate keys linking all dimensions |

Registered under **`apex_retail.GOLD_tables`** in Unity Catalog for standard SQL discovery and governance.

---

##  Business KPIs

Computed entirely inline in the Gold notebook — no Power BI, Tableau, or external dashboards used.

| # | KPI | Business Question Answered |
|---|---|---|
| 1️ | **Net Margin by Region** | Which regions are most profitable after discounts? |
| 2️ | **AOV by Promotion** | Which promotion types drive the highest cart values? |
| 3️ | **Demographic Churn Heatmap** | Where is churn concentrated by state & loyalty status? |
| 4️ | **Product Quality Index** | Which product categories have the highest return rates? |
| 5️ | **Store Traffic by Hour** | When are stores busiest — by hour and day of week? |

---

##  Data Quality & Engineering Highlights

Real issues were found, root-caused, and resolved during development:

-  **Join fan-out bug** — `dim_promotion` initially produced duplicate `promotion_id` rows, causing `fact_sales` to balloon from 2,000 → 4,406 rows. Diagnosed via step-by-step join tracing, fixed with a window-function deduplication.
-  **Product catalog gap** — ~1,291 product_ids referenced in sales don't exist in the product catalog (a genuine source data characteristic, verified via type/format checks and direct lookups). Handled with an **Unknown Product** fallback member, keeping `fact_sales` fully joinable.
-  **Data Quality Scorecard** (`dq_scorecard`) — a persistent, queryable table tracking dimension coverage percentage across every foreign key, not just a one-time observation.
-  **Dual-layer audit validation** — both Landing and Silver dynamically read their respective `audit_landing`/`audit_silver` files and halt the pipeline on any count mismatch.

---

##  Idempotency & Auditability

- **Idempotent:** Every Silver and Gold write uses `overwrite` mode against freshly recomputed data — re-running any notebook produces identical results, with no duplicate or drifting rows.
- **Auditable:** Delta Lake's `DESCRIBE HISTORY` provides a complete, timestamped version log of every write to every table — a built-in audit trail with zero extra logging infrastructure.

---

##  Repository Structure

```
Apex_Retail_Intelligence/
│
├── README.md
│
├── notebooks/
│   ├── 01_raw_landing.ipynb
│   ├── 02_bronze.ipynb
│   ├── 03_silver.ipynb
│   └── 04_gold_kpis.ipynb
│
└── report/
    └── Apex_Retail_Intelligence_Report.pdf
```

>  Execution screenshots, validation outputs, and KPI results are documented inline within the final report (`report/Apex_Retail_Intelligence_Report.pdf`).

---

##  How to Run

1. Upload all source CSVs and audit files into a Databricks Unity Catalog **Volume**
2. Open notebooks in order: `01_raw_landing` → `02_bronze` → `03_silver` → `04_gold_kpis`
3. Run each notebook top to bottom — each layer reads from the previous layer's registered tables
4. KPI outputs render inline in `04_gold_kpis.ipynb` — no additional setup required

---

## Future Enhancements

**Automated, Declarative Data Quality Framework**

The current pipeline enforces data quality through manually written PySpark logic — filtering out rows with missing primary keys, dropping duplicates, and filling nulls with defaults. This works, but it has a quiet limitation: when a row fails a rule, it simply disappears, with no record of what was dropped or why. This became especially apparent while debugging a small row-count discrepancy during Silver audit validation, which required manually tracing through multiple cleansing steps to pinpoint exactly where the numbers diverged — a process that would have been immediate and self-documenting had the rules been declarative and instrumented from the start.

A natural next step would be replacing this manual approach with a declarative data quality framework, such as Databricks' own **Delta Live Tables (DLT) expectations**. Each rule — missing primary key, duplicate record, invalid numeric value — would become an explicit expectation attached to the table definition rather than an imperative filter buried in code. The real benefit is that DLT expectations can **quarantine** failing rows into a separate table instead of silently discarding them, along with an automatically generated report showing exactly how many rows failed each rule on every run. This would directly solve the kind of issue this project encountered — instead of discovering a discrepancy after the fact through manual comparison, the pipeline would surface it automatically, on every run, as a first-class output.

---

## Technologies Used

`PySpark` · `Delta Lake` · `Databricks Notebooks` · `Unity Catalog` · `Delta MERGE` · `Window Functions` · `Spark SQL`

---

## Author

**Lakshita**
Data Engineering Intern — Celebal Technologies
CEI'26 Internship Programme | Major Project

---

<p align="center"><i>Built on Databricks · Powered by Delta Lake</i></p>
