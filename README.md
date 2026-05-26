# DE Transition — Starter Code Package
## For Informatica PowerCenter Developers switching to Data Engineering

---

## What's in this package

```
starter_code/
│
├── python_basics/
│   ├── day1_python_basics.py       ← Day 1: lists, dicts, loops, error handling, file I/O
│   └── day5_api_pipeline.py        ← Day 5: fetch Chennai weather API → transform → Parquet
│
├── pandas_exercises/
│   └── day2_pandas_etl.py          ← Day 2: full ETL with Pandas (Informatica mappings in comments)
│
├── pyspark_notebooks/
│   ├── 01_bronze_ingestion.py      ← Databricks notebook: raw ingest → Bronze Delta table
│   ├── 02_silver_transform.py      ← Databricks notebook: cleanse + MERGE → Silver Delta
│   └── 03_gold_aggregation.py      ← Databricks notebook: aggregate → 3 Gold tables
│
├── kafka/
│   ├── docker-compose.yml          ← Start Kafka + Zookeeper + Kafdrop UI with one command
│   ├── producer.py                 ← Synthetic transaction event producer
│   └── consumer.py                 ← Kafka consumer with offset commit
│
├── dbt_project/
│   ├── dbt_project.yml             ← Main dbt project config
│   ├── profiles.yml                ← Connection profiles (DuckDB / Databricks / Postgres)
│   └── models/
│       ├── staging/
│       │   ├── schema.yml          ← Source definitions + column tests
│       │   └── stg_trips.sql       ← Staging model: rename, filter, deduplicate
│       ├── intermediate/           ← Add your intermediate join models here
│       └── marts/
│           └── mart_daily_revenue.sql  ← Gold mart: daily revenue with rolling averages
│
├── great_expectations/
│   └── data_quality_checks.py      ← 15 data quality checks + quarantine logic
│
└── utils/
    └── sql_interview_practice.sql  ← 10 SQL window function patterns with explanations
```

---

## Quick Start — Day 1

```bash
# 1. Install Python dependencies
pip install pandas pyarrow requests faker confluent-kafka great-expectations

# 2. Run Day 1 basics
python python_basics/day1_python_basics.py

# 3. Run Pandas ETL
python pandas_exercises/day2_pandas_etl.py

# 4. Run API pipeline
python python_basics/day5_api_pipeline.py
```

---

## Quick Start — Databricks Notebooks (Week 2)

1. Open https://community.cloud.databricks.com
2. Click **Workspace → Import**
3. Import `pyspark_notebooks/01_bronze_ingestion.py`
4. Upload NYC Taxi parquet to DBFS: **Data → Add Data → Upload**
5. Update `SOURCE_PATH` in the notebook to match your upload path
6. Run all cells

---

## Quick Start — Kafka (Month 3 Week 9)

```bash
# 1. Start Kafka
cd kafka/
docker-compose up -d

# 2. Wait 30 seconds, then open Kafdrop UI
# http://localhost:9000

# 3. Create a topic
docker exec kafka kafka-topics --create \
  --topic transactions \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

# 4. In terminal 1: start producer
pip install confluent-kafka faker
python producer.py --count 100 --delay 0.5

# 5. In terminal 2: start consumer
python consumer.py --from-beginning

# 6. Stop Kafka when done
docker-compose down
```

---

## Quick Start — dbt (Month 2 Week 5)

```bash
# 1. Install dbt
pip install dbt-core dbt-duckdb

# 2. Copy profiles.yml to ~/.dbt/
cp dbt_project/profiles.yml ~/.dbt/profiles.yml

# 3. Go to dbt project folder
cd dbt_project/

# 4. Test connection
dbt debug

# 5. Run models
dbt run

# 6. Run tests
dbt test

# 7. Generate & open docs
dbt docs generate
dbt docs serve
# Open: http://localhost:8080
```

---

## Informatica → Code Mapping (Quick Reference)

| Informatica               | Python/PySpark Equivalent                        |
|---------------------------|--------------------------------------------------|
| Source Qualifier          | `pd.read_csv()` / `spark.read.parquet()`         |
| Filter Transformation     | `df[condition]` / `df.filter(condition)`         |
| Expression Transformation | `df.withColumn()` / `df["new"] = ...`            |
| Aggregator Transformation | `df.groupby().agg()` / `df.groupBy().agg()`      |
| Joiner Transformation     | `df.merge()` / `df.join()`                       |
| Update Strategy           | `MERGE INTO` (Delta Lake) / dbt snapshot         |
| Router Transformation     | Multiple `df.filter()` branches                  |
| Sorter Transformation     | `df.sort_values()` / `df.orderBy()`              |
| Sequence Generator        | `monotonically_increasing_id()` / `uuid4()`      |
| Workflow                  | Databricks Workflow / Azure Data Factory         |
| Session                   | Databricks Notebook / ADF Activity               |
| PowerCenter Repository    | Unity Catalog + GitHub                           |
| Workflow Monitor          | Databricks Job Run History / ADF Monitor         |

---

## Support

- All datasets: see the Study Materials Word document
- All learning links: see the Study Materials Word document
- Questions about any file: ask Claude in claude.ai
