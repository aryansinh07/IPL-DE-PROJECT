# IPL-DE-PROJECT
An end-to-end data engineering project that ingests IPL match data daily, processes only new matches, and stores curated datasets in Amazon S3.

## Project Overview

This pipeline uses the Cricsheet IPL JSON feed:

- Source: `https://cricsheet.org/downloads/ipl_json.zip`
- One JSON per match
- Daily incremental load driven by a manifest file (`processed_files.json`)

The pipeline creates and maintains:

- `match_df.csv` (match-level dataset)
- `ball_by_ball_df.csv` (delivery-level dataset)
- `processed_files.json` (incremental state/manifest)

## Tech Stack

- Python
- Apache Airflow
- AWS S3
- boto3
- pandas
- requests

## DAGs

### 1) Ingestion DAG

- File: `dags/ipl_incremental_dag.py`
- DAG ID: `ipl_incremental_dag`
- Schedule: `0 11 * * *` (11:00 AM, Asia/Kolkata)

Tasks:

1. `download_zip`
2. `transform_new_files`
3. `upload_to_s3`

### 2) Weather Mini Project

- File: `dags/weatherproj_dag.py`
- Included as a separate API ETL mini-project

## Incremental Logic

1. Download latest IPL zip to local temp (`/tmp/ipl_pipeline`).
2. Read `processed_files.json` from S3.
3. Compare zip JSON file names with already processed file names.
4. Process only new JSON files.
5. Append + deduplicate S3 CSV datasets.
6. Update manifest in S3.

## Data Standardization

The pipeline applies cleanup/normalization:

- Season normalization:
  - `2007/08 -> 2008`
  - `2009/10 -> 2010`
  - `2020/21 -> 2020`
- Team name harmonization:
  - `Delhi Daredevils -> Delhi Capitals`
  - `Kings XI Punjab -> Punjab Kings`
  - `Royal Challengers Bangalore -> Royal Challengers Bengaluru`
  - and other legacy mappings

## S3 Objects (Current)

- `s3://de-ipl-daily-bucket/match_df.csv`
- `s3://de-ipl-daily-bucket/ball_by_ball_df.csv`
- `s3://de-ipl-daily-bucket/processed_files.json`

## Local/Airflow Setup

```bash
source ~/venvs/airflow_3.2.0/bin/activate
export AIRFLOW_HOME=~/airflow
airflow db migrate
airflow standalone
```

Install dependencies in the same Airflow venv:

```bash
pip install pandas requests boto3
```

## Deploy DAG to EC2

From local machine:

```bash
scp -i "C:\Users\aryansingh\Downloads\em3keypair.pem" "C:\Users\aryansingh\Downloads\BigData\airflow\dags\ipl_incremental_dag.py" ubuntu@<EC2_PUBLIC_IP>:/home/ubuntu/airflow/dags/
```

## How to Validate

After a DAG run:

1. All tasks in Graph view should be green.
2. `processed_files.json` timestamp should update.
3. If new matches exist, row counts in `match_df.csv` and `ball_by_ball_df.csv` should increase.
4. If no new matches exist, manifest timestamp may update but row counts remain unchanged.

## Resume Highlights

- Built an Airflow-based incremental IPL ingestion pipeline on EC2.
- Implemented manifest-driven new-file processing and idempotent append/dedupe.
- Created match-level and ball-by-ball curated datasets in S3.
- Extended the pipeline with scheduled analytics workflow support.
