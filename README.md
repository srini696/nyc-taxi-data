# 🚕 NYC Taxi Data — AWS Data Pipeline

> **A production-grade, end-to-end data engineering pipeline** for ingesting, processing, and analyzing billions of NYC Taxi & Limousine Commission (TLC) trip records using AWS cloud infrastructure.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [Tech Stack](#3-tech-stack)
4. [Prerequisites](#4-prerequisites)
5. [AWS Infrastructure Setup](#5-aws-infrastructure-setup)
6. [Installation](#6-installation)
7. [Configuration](#7-configuration)
8. [Data Sources](#8-data-sources)
9. [Database Schema](#9-database-schema)
10. [Running the Pipeline](#10-running-the-pipeline)
11. [Querying the Data](#11-querying-the-data)
12. [Benchmark Results](#12-benchmark-results)
13. [Project Structure](#13-project-structure)
14. [Troubleshooting](#14-troubleshooting)
15. [Contributing](#15-contributing)
16. [References & Credits](#16-references--credits)

---

## 1. Project Overview

This project provides a complete data pipeline for the **New York City Taxi & Limousine Commission (TLC) Trip Record Dataset** — one of the largest and most well-known public datasets in data engineering, containing **3+ billion taxi and for-hire vehicle (FHV) trips** from 2009 to present.

The pipeline is built and deployed entirely on **AWS**, leveraging managed services for ingestion, storage, processing, and analytics. It supports both **batch** and exploratory **analytical workloads** using AWS S3, EC2, Redshift or PostgreSQL, and optional ClickHouse for columnar benchmarking.

### What this project does

- Downloads raw trip data (Parquet/CSV) from the NYC TLC and/or AWS Open Data Registry
- Cleans and transforms records for analytical use
- Loads data into a relational schema (PostgreSQL / AWS Redshift)
- Optionally exports to ClickHouse for high-performance OLAP benchmarking
- Supports spatial enrichment via NYC census tracts and taxi zone shapefiles
- Provides example SQL queries and analysis scripts

### Key Numbers

| Metric | Value |
|---|---|
| Total trips (approx.) | 3+ billion |
| Raw data size | 200–300 GB compressed |
| Uncompressed DB size | ~375 GB (with indexes) |
| Date range | January 2009 – present |
| Trip types | Yellow taxi, Green taxi, FHV (Uber, Lyft, Via, Juno), FHVHV |

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│  NYC TLC (nyc.gov)   AWS Open Data Registry (S3 Public Bucket) │
└───────────────────────────────┬─────────────────────────────────┘
                                │
                          Download / Sync
                                │
                    ┌───────────▼────────────┐
                    │     Amazon S3 Bucket   │
                    │  (Raw Parquet / CSV)   │
                    └───────────┬────────────┘
                                │
                     ETL / Transform Scripts
                    (Shell + Python + SQL)
                                │
              ┌─────────────────┼──────────────────┐
              │                 │                  │
   ┌──────────▼──────┐  ┌───────▼──────┐  ┌───────▼──────────┐
   │   AWS Redshift  │  │  PostgreSQL  │  │   ClickHouse     │
   │  (Analytics DW) │  │ (on EC2/RDS) │  │ (OLAP Benchmark) │
   └──────────┬──────┘  └───────┬──────┘  └───────┬──────────┘
              │                 │                  │
              └─────────────────┼──────────────────┘
                                │
                        SQL Analytics / BI
```

**Flow summary:**

1. Raw Parquet files are fetched from the TLC website or the AWS Open Data S3 bucket.
2. A data cleaning step removes malformed rows (bad coordinates, nulls, etc.).
3. Cleaned data is loaded into the chosen analytical backend (Redshift, PostgreSQL, or ClickHouse).
4. Spatial joins are performed against NYC taxi zone and census tract shapefiles.
5. SQL queries and R/Python scripts perform downstream analysis.

---

## 3. Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Cloud Platform | **AWS** | All infrastructure |
| Compute | **Amazon EC2** (r5.4xlarge or larger) | ETL processing |
| Object Storage | **Amazon S3** | Raw data staging |
| Data Warehouse | **Amazon Redshift** or **PostgreSQL on EC2/RDS** | Analytical storage |
| OLAP Engine | **ClickHouse** (optional) | High-performance benchmarking |
| Data Format | **Apache Parquet** (post-2022) / CSV (legacy) | Trip record files |
| Scripting | **Bash**, **Python 3**, **SQL** | ETL orchestration |
| Spatial | **PostGIS**, **Shapefiles** | Geo-enrichment |
| Analysis | **R**, **Python (pandas, geopandas)** | Exploration & visualization |
| OS | **Ubuntu 18.04 / 20.04 LTS** | EC2 AMI |

---

## 4. Prerequisites

### Local / CI machine

- `git`
- `curl` / `wget`
- AWS CLI v2 (`aws --version`)
- Python 3.8+ with `pip`
- `psql` client (optional, for local PostgreSQL connections)

### AWS account requirements

- IAM user/role with permissions for: EC2, S3, Redshift (if used), VPC, IAM
- A key pair for SSH into EC2 instances
- S3 bucket created in your target region (e.g., `us-east-1`)
- Sufficient EC2 quota for the recommended instance types

### Python dependencies

```bash
pip install -r requirements.txt
```

Core packages include:

```
boto3>=1.26
pandas>=1.5
pyarrow>=10.0
psycopg2-binary
geopandas
shapely
sqlalchemy
```

---

## 5. AWS Infrastructure Setup

### 5.1 EC2 Instance

The ETL workload is CPU and I/O intensive. Recommended instance specs:

| Use Case | Instance Type | vCPU | RAM | Storage |
|---|---|---|---|---|
| Development / Test | `r5.2xlarge` | 8 | 64 GB | 500 GB gp3 EBS |
| Production ETL | `r5.4xlarge` | 16 | 128 GB | 2 TB gp3 EBS |
| Full billion-row load | `r5.8xlarge` | 32 | 256 GB | 4 TB gp3 EBS |

Launch an instance using the AWS Console or CLI:

```bash
aws ec2 run-instances \
  --image-id ami-0c55b159cbfafe1f0 \   # Ubuntu 20.04 LTS (us-east-1)
  --instance-type r5.4xlarge \
  --key-name your-key-pair \
  --security-group-ids sg-xxxxxxxx \
  --subnet-id subnet-xxxxxxxx \
  --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":2000,"VolumeType":"gp3"}}]' \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=nyc-taxi-etl}]'
```

### 5.2 Mount and Prepare Storage

SSH into the instance and prepare the data volume:

```bash
# List attached volumes
lsblk

# Format and mount the data volume (adjust device name as needed)
sudo mkfs -t ext4 /dev/nvme1n1
sudo mkdir -p /Data
sudo mount /dev/nvme1n1 /Data
sudo chown -R ubuntu:ubuntu /Data

# Make mount persistent across reboots
echo '/dev/nvme1n1 /Data ext4 defaults,nofail 0 2' | sudo tee -a /etc/fstab
```

### 5.3 S3 Bucket Setup

```bash
# Create a bucket for your raw data
aws s3 mb s3://your-nyc-taxi-raw-data --region us-east-1

# (Optional) Sync from the public TLC bucket
aws s3 sync s3://nyc-tlc/trip\ data/ s3://your-nyc-taxi-raw-data/trip-data/ \
  --no-sign-request \
  --region us-east-1
```

### 5.4 Security Group Rules

Ensure the EC2 security group allows:

| Port | Protocol | Source | Purpose |
|---|---|---|---|
| 22 | TCP | Your IP | SSH |
| 5432 | TCP | VPC CIDR | PostgreSQL |
| 8123 | TCP | VPC CIDR | ClickHouse HTTP |
| 9000 | TCP | VPC CIDR | ClickHouse Native |

---

## 6. Installation

### 6.1 Clone the Repository

```bash
git clone https://github.com/srini696/nyc-taxi-data.git
cd nyc-taxi-data
```

### 6.2 Install System Dependencies

```bash
sudo apt-get update && sudo apt-get install -y \
  postgresql-14 \
  postgresql-14-postgis-3 \
  python3 python3-pip \
  gdal-bin \
  libgdal-dev \
  unzip \
  curl \
  wget \
  awscli
```

### 6.3 Install Python Packages

```bash
pip3 install -r requirements.txt
```

### 6.4 Configure PostgreSQL

PostgreSQL's default data directory is on the root partition, which will fill up. Redirect it to your large data volume:

```bash
# Stop PostgreSQL
sudo systemctl stop postgresql

# Create the new data directory on large volume
sudo mkdir -p /Data/PGDATA
sudo chown -R postgres:postgres /Data/PGDATA
sudo chmod 700 /Data/PGDATA

# Copy existing data
sudo cp -av /var/lib/postgresql/14/main/* /Data/PGDATA/

# Update postgresql.conf
sudo nano /etc/postgresql/14/main/postgresql.conf
# Change: data_directory = '/Data/PGDATA'

# Restart
sudo systemctl start postgresql
sudo systemctl status postgresql
```

Create a superuser for the ETL scripts:

```bash
sudo su - postgres -c \
  "psql -c 'CREATE USER ubuntu; ALTER USER ubuntu WITH SUPERUSER;'"
```

Create the database:

```bash
createdb nyc_taxi
psql nyc_taxi -c "CREATE EXTENSION postgis;"
psql nyc_taxi -c "CREATE EXTENSION postgis_topology;"
```

---

## 7. Configuration

Copy and edit the environment configuration file:

```bash
cp config/settings.env.example config/settings.env
nano config/settings.env
```

Key variables:

```bash
# AWS
AWS_REGION=us-east-1
S3_BUCKET=your-nyc-taxi-raw-data
S3_PREFIX=trip-data/

# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=nyc_taxi
DB_USER=ubuntu
DB_PASSWORD=

# Data paths
RAW_DATA_DIR=/Data/nyc-taxi-raw
PROCESSED_DATA_DIR=/Data/nyc-taxi-processed

# ClickHouse (optional)
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9000
CLICKHOUSE_DB=nyc_taxi
```

Export these before running scripts:

```bash
source config/settings.env
export $(cut -d= -f1 config/settings.env)
```

---

## 8. Data Sources

### 8.1 NYC TLC Trip Record Data

The raw data is published by the **NYC Taxi & Limousine Commission** and hosted on both the TLC's website and the **AWS Open Data Registry**.

| Source | Format | Coverage |
|---|---|---|
| NYC TLC website (`nyc.gov`) | Parquet (2022+), CSV (legacy) | 2009–present |
| AWS Open Data S3 (`s3://nyc-tlc/`) | Parquet | 2009–present |

> **Note:** The TLC migrated from CSV to Apache Parquet format in May 2022, including backfilling all historical files.

### 8.2 Trip Types

| Type | Table | Description |
|---|---|---|
| Yellow Taxi | `trips` | Traditional metered yellow cabs |
| Green Taxi | `trips` | Outer-borough street hail livery |
| FHV | `fhv_trips` | For-hire vehicles: Uber, Lyft, Via, Juno |
| FHVHV | `fhvhv_trips` | High-volume FHV (Uber, Lyft post-2019) |

### 8.3 Supplementary Data

The following reference datasets are bundled with the repository (no separate download needed):

- **NYC Census Tracts (2010)** — from NYC Department of City Planning (Bytes of the Big Apple)
- **Taxi Zone Shapefiles** — from the NYC TLC
- **Central Park Weather Observations** — from the National Climatic Data Center
- **FHV Base Number Mappings** — from the NYC TLC

---

## 9. Database Schema

### Core Tables

#### `trips` — Yellow and Green Taxi Trips

```sql
CREATE TABLE trips (
    id                          BIGSERIAL PRIMARY KEY,
    cab_type_id                 INTEGER,
    vendor_id                   VARCHAR(3),
    pickup_datetime             TIMESTAMP WITHOUT TIME ZONE,
    dropoff_datetime            TIMESTAMP WITHOUT TIME ZONE,
    store_and_fwd_flag          VARCHAR(1),
    rate_code_id                INTEGER,
    pickup_longitude            NUMERIC(18,14),
    pickup_latitude             NUMERIC(18,14),
    dropoff_longitude           NUMERIC(18,14),
    dropoff_latitude            NUMERIC(18,14),
    passenger_count             INTEGER,
    trip_distance               NUMERIC(18,4),
    fare_amount                 NUMERIC(18,4),
    extra                       NUMERIC(18,4),
    mta_tax                     NUMERIC(18,4),
    tip_amount                  NUMERIC(18,4),
    tolls_amount                NUMERIC(18,4),
    ehail_fee                   NUMERIC(18,4),
    improvement_surcharge       NUMERIC(18,4),
    total_amount                NUMERIC(18,4),
    payment_type                VARCHAR(10),
    trip_type                   INTEGER,
    pickup_nyct2010_gid         INTEGER REFERENCES nyct2010(gid),
    dropoff_nyct2010_gid        INTEGER REFERENCES nyct2010(gid),
    pickup_taxizone_id          INTEGER REFERENCES taxi_zones(locationid),
    dropoff_taxizone_id         INTEGER REFERENCES taxi_zones(locationid)
);
```

#### `fhv_trips` — For-Hire Vehicle Trips

```sql
CREATE TABLE fhv_trips (
    id                          BIGSERIAL PRIMARY KEY,
    dispatching_base_num        VARCHAR(6),
    pickup_datetime             TIMESTAMP WITHOUT TIME ZONE,
    dropoff_datetime            TIMESTAMP WITHOUT TIME ZONE,
    pickup_taxizone_id          INTEGER REFERENCES taxi_zones(locationid),
    dropoff_taxizone_id         INTEGER REFERENCES taxi_zones(locationid),
    sr_flag                     INTEGER
);
```

#### `taxi_zones` — TLC Taxi Zone Boundaries

```sql
CREATE TABLE taxi_zones (
    locationid  INTEGER PRIMARY KEY,
    borough     VARCHAR(50),
    zone        VARCHAR(100),
    service_zone VARCHAR(50)
);
```

#### `cab_types`

```sql
CREATE TABLE cab_types (
    id    SERIAL PRIMARY KEY,
    type  VARCHAR(20)
);
-- Values: 'yellow', 'green'
```

#### `central_park_weather_observations`

```sql
CREATE TABLE central_park_weather_observations (
    date               DATE PRIMARY KEY,
    precipitation      NUMERIC(6,2),
    snow_depth         NUMERIC(6,2),
    snowfall           NUMERIC(6,2),
    max_temperature    INTEGER,
    min_temperature    INTEGER,
    average_wind_speed NUMERIC(6,2)
);
```

---

## 10. Running the Pipeline

### Step 1 — Initialize the Schema

```bash
psql nyc_taxi -f setup_files/schema.sql
psql nyc_taxi -f setup_files/seed_files/taxi_zone_lookups.csv  # or via COPY
```

### Step 2 — Download Raw Data

Download all trip data types (this will take several hours depending on your connection):

```bash
./download_raw_data.sh
```

Or download selectively by trip type:

```bash
./import_yellow_taxi_trip_data.sh
./import_green_taxi_trip_data.sh
./import_fhv_taxi_trip_data.sh
./import_fhvhv_trip_data.sh
```

To sync from the AWS Open Data Registry instead:

```bash
aws s3 sync s3://nyc-tlc/trip\ data/ /Data/nyc-taxi-raw/ --no-sign-request
```

### Step 3 — Remove Bad Rows

```bash
./remove_bad_rows.sh
```

This script strips rows with:
- Invalid or null coordinates
- Negative fare amounts
- Impossible trip durations
- Corrupted fields

### Step 4 — Load into PostgreSQL

```bash
./import_trip_data.sh
```

> ⚠️ **This process can take 12–48 hours** for the full dataset depending on instance size.

Monitor progress:

```bash
psql nyc_taxi -c "
  SELECT relname AS table_name,
         lpad(to_char(reltuples, 'FM9,999,999,999'), 13) AS row_count
  FROM pg_class
  LEFT JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
  WHERE nspname NOT IN ('pg_catalog', 'information_schema')
    AND relkind = 'r'
  ORDER BY reltuples DESC;
"
```

### Step 5 — Spatial Enrichment (Optional)

```bash
./geocode_pickups_and_dropoffs.sh
```

This uses PostGIS to map lat/lon coordinates to NYC census tracts and taxi zones.

### Step 6 — Export to ClickHouse (Optional)

First export from PostgreSQL to compressed CSV files:

```bash
mkdir -p /Data/nyc-taxi-trips-export
sudo chown -R postgres:postgres /Data/nyc-taxi-trips-export

psql nyc_taxi -c "\COPY trips TO PROGRAM 'split -l 20000000 --filter=\"gzip > /Data/nyc-taxi-trips-export/trips_\$FILE.csv.gz\" --additional-suffix=.csv' WITH CSV"
```

Then load into ClickHouse:

```bash
time (for filename in /Data/nyc-taxi-trips-export/trips_*.csv.gz; do
  echo "$filename"
  gunzip -ck "$filename" | \
  python3 transform.py | \
  clickhouse-client --query="INSERT INTO trips FORMAT CSV"
done)
```

---

## 11. Querying the Data

### Example Queries

#### Total trips by cab type

```sql
SELECT c.type AS cab_type, COUNT(*) AS trip_count
FROM trips t
JOIN cab_types c ON t.cab_type_id = c.id
GROUP BY c.type
ORDER BY trip_count DESC;
```

#### Average fare and tip by payment type

```sql
SELECT payment_type,
       COUNT(*)                       AS trips,
       ROUND(AVG(fare_amount), 2)     AS avg_fare,
       ROUND(AVG(tip_amount), 2)      AS avg_tip,
       ROUND(AVG(tip_amount /
             NULLIF(fare_amount, 0)) * 100, 1) AS tip_pct
FROM trips
WHERE fare_amount > 0
GROUP BY payment_type
ORDER BY trips DESC;
```

#### Top pickup neighborhoods

```sql
SELECT n.ntaname AS neighborhood,
       COUNT(*) AS pickups
FROM trips t
JOIN nyct2010 n ON t.pickup_nyct2010_gid = n.gid
GROUP BY n.ntaname
ORDER BY pickups DESC
LIMIT 20;
```

#### Trips by hour of day (with weather join)

```sql
SELECT EXTRACT(HOUR FROM t.pickup_datetime) AS hour_of_day,
       COUNT(*) AS trips,
       AVG(w.precipitation) AS avg_precip
FROM trips t
LEFT JOIN central_park_weather_observations w
  ON DATE(t.pickup_datetime) = w.date
GROUP BY hour_of_day
ORDER BY hour_of_day;
```

#### Daily trip volume trend

```sql
SELECT DATE(pickup_datetime) AS trip_date,
       COUNT(*) AS daily_trips
FROM trips
WHERE pickup_datetime BETWEEN '2022-01-01' AND '2022-12-31'
GROUP BY trip_date
ORDER BY trip_date;
```

---

## 12. Benchmark Results

Benchmarks were run on an **AWS EC2 r5.4xlarge** instance (16 vCPU, 128 GB RAM) with a 2 TB gp3 EBS volume.

### PostgreSQL Load Performance

| Task | Duration |
|---|---|
| Download raw data (200 GB) | ~3–6 hours |
| Data cleaning | ~1–2 hours |
| PostgreSQL import | ~12–36 hours |
| Index creation | ~4–8 hours |
| Geocoding (optional) | ~8–24 hours |

### ClickHouse Query Benchmarks (1.1B rows)

| Query | ClickHouse | PostgreSQL |
|---|---|---|
| Total row count | ~0.1s | ~5s |
| Group by cab_type | ~0.5s | ~120s |
| Average fare by year | ~1.2s | ~180s |
| Pickup neighborhood counts | ~2.1s | ~300s |

> ClickHouse delivers **50–200x** query speedups over PostgreSQL for large aggregations on this dataset.

---

## 13. Project Structure

```
nyc-taxi-data/
├── README.md
├── requirements.txt
├── config/
│   └── settings.env.example
├── setup_files/
│   ├── schema.sql                  # PostgreSQL DDL
│   ├── schema_clickhouse.sql       # ClickHouse DDL
│   └── seed_files/
│       ├── taxi_zone_lookups.csv
│       └── fhv_bases.csv
├── download_raw_data.sh            # Downloads all TLC data
├── remove_bad_rows.sh              # Cleans corrupt records
├── import_trip_data.sh             # Main PostgreSQL loader
├── import_yellow_taxi_trip_data.sh
├── import_green_taxi_trip_data.sh
├── import_fhv_taxi_trip_data.sh
├── import_fhvhv_trip_data.sh
├── geocode_pickups_and_dropoffs.sh # PostGIS spatial enrichment
├── transform.py                    # CSV transform for ClickHouse
├── analysis/
│   ├── queries.sql                 # Example SQL analyses
│   ├── analysis.R                  # R visualizations
│   └── graphs/                     # Output charts
└── shapefiles/
    ├── nyct2010/                   # NYC census tracts
    └── taxi_zones/                 # TLC taxi zone boundaries
```

---

## 14. Troubleshooting

### PostgreSQL runs out of disk space

The default data directory `/var/lib/postgresql/` may be on a small root partition.

**Solution:** Redirect `data_directory` in `postgresql.conf` to your large EBS volume (see [Section 6.4](#64-configure-postgresql)).

### Import script aborts with `ERROR: extra data after last expected column`

Some TLC raw CSV files contain extra columns.

**Solution:** `remove_bad_rows.sh` handles this. Re-run it before importing, or manually remove the offending rows:

```bash
grep -v "^$" raw_file.csv | awk -F',' 'NF==17' > cleaned_file.csv
```

### AWS CLI credential errors

```
An error occurred (InvalidClientTokenId) when calling the ... operation
```

**Solution:** Reconfigure AWS credentials:

```bash
aws configure
# Enter: Access Key ID, Secret Access Key, Region (us-east-1), Output (json)
```

### ClickHouse insert fails mid-stream

Long-running bulk inserts can timeout.

**Solution:** Increase ClickHouse timeouts:

```bash
clickhouse-client \
  --receive_timeout=3600 \
  --send_timeout=3600 \
  --query="INSERT INTO trips FORMAT CSV"
```

### PostGIS geocoding is extremely slow

**Solution:** Ensure PostGIS spatial indexes are created before running geocoding:

```sql
CREATE INDEX ON nyct2010 USING GIST (geom);
CREATE INDEX ON taxi_zones USING GIST (geom);
VACUUM ANALYZE nyct2010;
VACUUM ANALYZE taxi_zones;
```

---

## 15. Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/my-improvement`
3. Commit your changes with clear messages: `git commit -m "Add: support for 2024 FHVHV Parquet schema"`
4. Push and open a Pull Request against `main`

### Code style

- Shell scripts: follow Google Shell Style Guide, use `set -euo pipefail`
- Python: PEP 8, type hints encouraged, run `black` before committing
- SQL: uppercase keywords, lowercase identifiers, 4-space indents

---

## 16. References & Credits

| Resource | Link |
|---|---|
| NYC TLC Trip Record Data | https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page |
| AWS Open Data Registry — NYC TLC | https://registry.opendata.aws/nyc-tlc-trip-records-pds/ |
| Original pipeline (Todd Schneider) | https://github.com/toddwschneider/nyc-taxi-data |
| ClickHouse benchmark (Mark Litwintschik) | https://tech.marksblogg.com/benchmarks.html |
| NYC Census Tract Shapefiles | https://www.nyc.gov/site/planning/data-maps/open-data.page |
| ClickHouse Documentation | https://clickhouse.com/docs |
| PostGIS Documentation | https://postgis.net/documentation/ |

---

## License

This project is open source. The underlying NYC TLC trip data is published as open data by the City of New York under the [NYC Open Data Terms of Use](https://opendata.cityofnewyork.us/overview/#termsofuse).

---

*Last updated: April 2026 | Maintained by [@srini696](https://github.com/srini696)*
