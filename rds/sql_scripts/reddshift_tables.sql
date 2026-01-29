-- ============================================================================
-- NYC TAXI DATA WAREHOUSE - REDSHIFT COMPATIBLE DDL
-- ============================================================================
-- All syntax verified for Amazon Redshift
-- Run these commands in order
-- ============================================================================

-- ============================================================================
-- STEP 1: CREATE SCHEMA
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS nyc_taxi_dw;

-- Set search path for this session
SET search_path TO nyc_taxi_dw, public;

-- ============================================================================
-- STEP 2: DROP EXISTING TABLES (if re-running)
-- ============================================================================
DROP TABLE IF EXISTS nyc_taxi_dw.fact_trip CASCADE;
DROP TABLE IF EXISTS nyc_taxi_dw.agg_daily_summary CASCADE;
DROP TABLE IF EXISTS nyc_taxi_dw.agg_hourly_zone CASCADE;
DROP TABLE IF EXISTS nyc_taxi_dw.stg_yellow_trips CASCADE;
DROP TABLE IF EXISTS nyc_taxi_dw.dim_vendor CASCADE;
DROP TABLE IF EXISTS nyc_taxi_dw.dim_zone CASCADE;
DROP TABLE IF EXISTS nyc_taxi_dw.dim_ratecode CASCADE;
DROP TABLE IF EXISTS nyc_taxi_dw.dim_payment CASCADE;
DROP TABLE IF EXISTS nyc_taxi_dw.dim_date CASCADE;
DROP TABLE IF EXISTS nyc_taxi_dw.dim_time CASCADE;

-- ============================================================================
-- STEP 3: CREATE DIMENSION TABLES
-- ============================================================================

-- ---------------------------------------------------------------------------
-- DIM_VENDOR
-- ---------------------------------------------------------------------------
CREATE TABLE nyc_taxi_dw.dim_vendor (
    vendor_sk       SMALLINT IDENTITY(1,1),
    vendor_id       SMALLINT NOT NULL,
    vendor_name     VARCHAR(100) NOT NULL,
    vendor_code     VARCHAR(10),
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT SYSDATE,
    PRIMARY KEY (vendor_sk)
)
DISTSTYLE ALL;

-- ---------------------------------------------------------------------------
-- DIM_ZONE
-- ---------------------------------------------------------------------------
CREATE TABLE nyc_taxi_dw.dim_zone (
    zone_sk         SMALLINT IDENTITY(1,1),
    location_id     SMALLINT NOT NULL,
    borough         VARCHAR(50) NOT NULL,
    zone_name       VARCHAR(100) NOT NULL,
    service_zone    VARCHAR(50) NOT NULL,
    is_airport      BOOLEAN DEFAULT FALSE,
    is_manhattan    BOOLEAN DEFAULT FALSE,
    is_yellow_zone  BOOLEAN DEFAULT FALSE,
    zone_category   VARCHAR(30),
    created_at      TIMESTAMP DEFAULT SYSDATE,
    PRIMARY KEY (zone_sk)
)
DISTSTYLE ALL;

-- ---------------------------------------------------------------------------
-- DIM_RATECODE
-- ---------------------------------------------------------------------------
CREATE TABLE nyc_taxi_dw.dim_ratecode (
    ratecode_sk         SMALLINT IDENTITY(1,1),
    ratecode_id         SMALLINT NOT NULL,
    rate_code_name      VARCHAR(50) NOT NULL,
    rate_description    VARCHAR(200),
    is_flat_rate        BOOLEAN DEFAULT FALSE,
    is_negotiated       BOOLEAN DEFAULT FALSE,
    created_at          TIMESTAMP DEFAULT SYSDATE,
    PRIMARY KEY (ratecode_sk)
)
DISTSTYLE ALL;

-- ---------------------------------------------------------------------------
-- DIM_PAYMENT
-- ---------------------------------------------------------------------------
CREATE TABLE nyc_taxi_dw.dim_payment (
    payment_sk          SMALLINT IDENTITY(1,1),
    payment_type_id     SMALLINT NOT NULL,
    payment_type_name   VARCHAR(50) NOT NULL,
    payment_category    VARCHAR(30),
    allows_tip_record   BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMP DEFAULT SYSDATE,
    PRIMARY KEY (payment_sk)
)
DISTSTYLE ALL;

-- ---------------------------------------------------------------------------
-- DIM_DATE
-- ---------------------------------------------------------------------------
CREATE TABLE nyc_taxi_dw.dim_date (
    date_sk             INTEGER NOT NULL,
    full_date           DATE NOT NULL,
    day_of_week         SMALLINT NOT NULL,
    day_of_week_name    VARCHAR(10) NOT NULL,
    day_of_month        SMALLINT NOT NULL,
    day_of_year         SMALLINT NOT NULL,
    week_of_year        SMALLINT NOT NULL,
    month_number        SMALLINT NOT NULL,
    month_name          VARCHAR(10) NOT NULL,
    month_short         VARCHAR(3) NOT NULL,
    quarter             SMALLINT NOT NULL,
    quarter_name        VARCHAR(2) NOT NULL,
    year                SMALLINT NOT NULL,
    year_month          VARCHAR(7) NOT NULL,
    is_weekend          BOOLEAN NOT NULL,
    is_holiday          BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (date_sk)
)
DISTSTYLE ALL
SORTKEY (full_date);

-- ---------------------------------------------------------------------------
-- DIM_TIME
-- ---------------------------------------------------------------------------
CREATE TABLE nyc_taxi_dw.dim_time (
    time_sk             SMALLINT NOT NULL,
    hour_24             SMALLINT NOT NULL,
    hour_12             SMALLINT NOT NULL,
    am_pm               VARCHAR(2) NOT NULL,
    time_period         VARCHAR(20) NOT NULL,
    rush_hour_flag      BOOLEAN NOT NULL,
    day_part            VARCHAR(15) NOT NULL,
    business_hour       BOOLEAN NOT NULL,
    PRIMARY KEY (time_sk)
)
DISTSTYLE ALL
SORTKEY (hour_24);

-- ============================================================================
-- STEP 4: CREATE STAGING TABLE
-- ============================================================================

CREATE TABLE nyc_taxi_dw.stg_yellow_trips (
    vendorid                SMALLINT,
    tpep_pickup_datetime    TIMESTAMP,
    tpep_dropoff_datetime   TIMESTAMP,
    passenger_count         FLOAT,
    trip_distance           FLOAT,
    ratecodeid              SMALLINT,
    store_and_fwd_flag      VARCHAR(1),
    pulocationid            SMALLINT,
    dolocationid            SMALLINT,
    payment_type            SMALLINT,
    fare_amount             FLOAT,
    extra                   FLOAT,
    mta_tax                 FLOAT,
    tip_amount              FLOAT,
    tolls_amount            FLOAT,
    improvement_surcharge   FLOAT,
    total_amount            FLOAT,
    congestion_surcharge    FLOAT,
    airport_fee             FLOAT,
    cbd_congestion_fee      FLOAT
);

-- ============================================================================
-- STEP 5: CREATE FACT TABLE
-- ============================================================================

CREATE TABLE nyc_taxi_dw.fact_trip (
    trip_sk                 BIGINT IDENTITY(1,1),
    pickup_date_sk          INTEGER NOT NULL,
    pickup_time_sk          SMALLINT NOT NULL,
    dropoff_date_sk         INTEGER NOT NULL,
    dropoff_time_sk         SMALLINT NOT NULL,
    vendor_sk               SMALLINT NOT NULL,
    pickup_zone_sk          SMALLINT NOT NULL,
    dropoff_zone_sk         SMALLINT NOT NULL,
    ratecode_sk             SMALLINT NOT NULL,
    payment_sk              SMALLINT NOT NULL,
    store_and_fwd_flag      VARCHAR(1),
    passenger_count         SMALLINT,
    trip_distance           FLOAT,
    fare_amount             FLOAT,
    extra_amount            FLOAT,
    mta_tax                 FLOAT,
    tip_amount              FLOAT,
    tolls_amount            FLOAT,
    improvement_surcharge   FLOAT,
    congestion_surcharge    FLOAT,
    airport_fee             FLOAT,
    cbd_congestion_fee      FLOAT,
    total_amount            FLOAT,
    trip_duration_minutes   FLOAT,
    tip_percentage          FLOAT,
    is_long_trip            BOOLEAN,
    is_short_trip           BOOLEAN,
    is_high_tip             BOOLEAN,
    is_cash_trip            BOOLEAN,
    is_airport_trip         BOOLEAN,
    is_rush_hour            BOOLEAN,
    etl_load_timestamp      TIMESTAMP DEFAULT SYSDATE,
    PRIMARY KEY (trip_sk)
)
DISTSTYLE KEY
DISTKEY (pickup_date_sk)
COMPOUND SORTKEY (pickup_date_sk, pickup_time_sk, vendor_sk, pickup_zone_sk);

-- ============================================================================
-- STEP 6: CREATE AGGREGATE TABLES
-- ============================================================================

CREATE TABLE nyc_taxi_dw.agg_daily_summary (
    date_sk                 INTEGER NOT NULL,
    vendor_sk               SMALLINT NOT NULL,
    pickup_zone_sk          SMALLINT NOT NULL,
    trip_count              INTEGER NOT NULL,
    total_passengers        INTEGER,
    total_distance          FLOAT,
    total_fare              FLOAT,
    total_tips              FLOAT,
    total_revenue           FLOAT,
    avg_trip_distance       FLOAT,
    avg_fare                FLOAT,
    avg_tip_percentage      FLOAT,
    avg_trip_duration       FLOAT,
    cash_trip_count         INTEGER,
    card_trip_count         INTEGER,
    airport_trip_count      INTEGER,
    rush_hour_trip_count    INTEGER,
    etl_load_timestamp      TIMESTAMP DEFAULT SYSDATE,
    PRIMARY KEY (date_sk, vendor_sk, pickup_zone_sk)
)
DISTSTYLE KEY
DISTKEY (date_sk)
SORTKEY (date_sk, vendor_sk);

CREATE TABLE nyc_taxi_dw.agg_hourly_zone (
    date_sk                 INTEGER NOT NULL,
    time_sk                 SMALLINT NOT NULL,
    zone_sk                 SMALLINT NOT NULL,
    pickup_count            INTEGER DEFAULT 0,
    dropoff_count           INTEGER DEFAULT 0,
    net_flow                INTEGER,
    total_fare              FLOAT,
    avg_fare                FLOAT,
    PRIMARY KEY (date_sk, time_sk, zone_sk)
)
DISTSTYLE KEY
DISTKEY (date_sk)
SORTKEY (date_sk, time_sk, zone_sk);

-- ============================================================================
-- STEP 7: SEED DIMENSION DATA
-- ============================================================================

-- Vendor Dimension
INSERT INTO nyc_taxi_dw.dim_vendor (vendor_id, vendor_name, vendor_code) VALUES
(1, 'Creative Mobile Technologies, LLC', 'CMT');
INSERT INTO nyc_taxi_dw.dim_vendor (vendor_id, vendor_name, vendor_code) VALUES
(2, 'Curb Mobility, LLC', 'CURB');
INSERT INTO nyc_taxi_dw.dim_vendor (vendor_id, vendor_name, vendor_code) VALUES
(6, 'Myle Technologies Inc', 'MYLE');
INSERT INTO nyc_taxi_dw.dim_vendor (vendor_id, vendor_name, vendor_code) VALUES
(7, 'Helix', 'HELIX');
INSERT INTO nyc_taxi_dw.dim_vendor (vendor_id, vendor_name, vendor_code) VALUES
(0, 'Unknown Vendor', 'UNK');

-- Ratecode Dimension
INSERT INTO nyc_taxi_dw.dim_ratecode (ratecode_id, rate_code_name, rate_description, is_flat_rate, is_negotiated) VALUES
(1, 'Standard Rate', 'Standard metered fare', FALSE, FALSE);
INSERT INTO nyc_taxi_dw.dim_ratecode (ratecode_id, rate_code_name, rate_description, is_flat_rate, is_negotiated) VALUES
(2, 'JFK', 'Flat fare to/from JFK Airport', TRUE, FALSE);
INSERT INTO nyc_taxi_dw.dim_ratecode (ratecode_id, rate_code_name, rate_description, is_flat_rate, is_negotiated) VALUES
(3, 'Newark', 'Newark Airport fare', FALSE, FALSE);
INSERT INTO nyc_taxi_dw.dim_ratecode (ratecode_id, rate_code_name, rate_description, is_flat_rate, is_negotiated) VALUES
(4, 'Nassau/Westchester', 'Trips to Nassau or Westchester', FALSE, FALSE);
INSERT INTO nyc_taxi_dw.dim_ratecode (ratecode_id, rate_code_name, rate_description, is_flat_rate, is_negotiated) VALUES
(5, 'Negotiated Fare', 'Pre-negotiated flat fare', TRUE, TRUE);
INSERT INTO nyc_taxi_dw.dim_ratecode (ratecode_id, rate_code_name, rate_description, is_flat_rate, is_negotiated) VALUES
(6, 'Group Ride', 'Shared ride fare', FALSE, FALSE);
INSERT INTO nyc_taxi_dw.dim_ratecode (ratecode_id, rate_code_name, rate_description, is_flat_rate, is_negotiated) VALUES
(99, 'Unknown', 'Unknown or null rate code', FALSE, FALSE);

-- Payment Dimension
INSERT INTO nyc_taxi_dw.dim_payment (payment_type_id, payment_type_name, payment_category, allows_tip_record) VALUES
(1, 'Credit Card', 'ELECTRONIC', TRUE);
INSERT INTO nyc_taxi_dw.dim_payment (payment_type_id, payment_type_name, payment_category, allows_tip_record) VALUES
(2, 'Cash', 'CASH', FALSE);
INSERT INTO nyc_taxi_dw.dim_payment (payment_type_id, payment_type_name, payment_category, allows_tip_record) VALUES
(3, 'No Charge', 'OTHER', FALSE);
INSERT INTO nyc_taxi_dw.dim_payment (payment_type_id, payment_type_name, payment_category, allows_tip_record) VALUES
(4, 'Dispute', 'OTHER', FALSE);
INSERT INTO nyc_taxi_dw.dim_payment (payment_type_id, payment_type_name, payment_category, allows_tip_record) VALUES
(5, 'Unknown', 'OTHER', FALSE);
INSERT INTO nyc_taxi_dw.dim_payment (payment_type_id, payment_type_name, payment_category, allows_tip_record) VALUES
(6, 'Voided Trip', 'OTHER', FALSE);
INSERT INTO nyc_taxi_dw.dim_payment (payment_type_id, payment_type_name, payment_category, allows_tip_record) VALUES
(0, 'Not Recorded', 'OTHER', FALSE);

-- Time Dimension (24 hours)
INSERT INTO nyc_taxi_dw.dim_time VALUES (0, 0, 12, 'AM', 'Late Night', FALSE, 'Night', FALSE);
INSERT INTO nyc_taxi_dw.dim_time VALUES (1, 1, 1, 'AM', 'Late Night', FALSE, 'Night', FALSE);
INSERT INTO nyc_taxi_dw.dim_time VALUES (2, 2, 2, 'AM', 'Late Night', FALSE, 'Night', FALSE);
INSERT INTO nyc_taxi_dw.dim_time VALUES (3, 3, 3, 'AM', 'Late Night', FALSE, 'Night', FALSE);
INSERT INTO nyc_taxi_dw.dim_time VALUES (4, 4, 4, 'AM', 'Late Night', FALSE, 'Night', FALSE);
INSERT INTO nyc_taxi_dw.dim_time VALUES (5, 5, 5, 'AM', 'Early Morning', FALSE, 'Dawn', FALSE);
INSERT INTO nyc_taxi_dw.dim_time VALUES (6, 6, 6, 'AM', 'Early Morning', FALSE, 'Dawn', FALSE);
INSERT INTO nyc_taxi_dw.dim_time VALUES (7, 7, 7, 'AM', 'Morning Rush', TRUE, 'Morning', FALSE);
INSERT INTO nyc_taxi_dw.dim_time VALUES (8, 8, 8, 'AM', 'Morning Rush', TRUE, 'Morning', FALSE);
INSERT INTO nyc_taxi_dw.dim_time VALUES (9, 9, 9, 'AM', 'Morning', TRUE, 'Morning', TRUE);
INSERT INTO nyc_taxi_dw.dim_time VALUES (10, 10, 10, 'AM', 'Morning', FALSE, 'Morning', TRUE);
INSERT INTO nyc_taxi_dw.dim_time VALUES (11, 11, 11, 'AM', 'Morning', FALSE, 'Morning', TRUE);
INSERT INTO nyc_taxi_dw.dim_time VALUES (12, 12, 12, 'PM', 'Midday', FALSE, 'Afternoon', TRUE);
INSERT INTO nyc_taxi_dw.dim_time VALUES (13, 13, 1, 'PM', 'Midday', FALSE, 'Afternoon', TRUE);
INSERT INTO nyc_taxi_dw.dim_time VALUES (14, 14, 2, 'PM', 'Afternoon', FALSE, 'Afternoon', TRUE);
INSERT INTO nyc_taxi_dw.dim_time VALUES (15, 15, 3, 'PM', 'Afternoon', FALSE, 'Afternoon', TRUE);
INSERT INTO nyc_taxi_dw.dim_time VALUES (16, 16, 4, 'PM', 'Afternoon', FALSE, 'Afternoon', TRUE);
INSERT INTO nyc_taxi_dw.dim_time VALUES (17, 17, 5, 'PM', 'Evening Rush', TRUE, 'Evening', TRUE);
INSERT INTO nyc_taxi_dw.dim_time VALUES (18, 18, 6, 'PM', 'Evening Rush', TRUE, 'Evening', FALSE);
INSERT INTO nyc_taxi_dw.dim_time VALUES (19, 19, 7, 'PM', 'Evening', TRUE, 'Evening', FALSE);
INSERT INTO nyc_taxi_dw.dim_time VALUES (20, 20, 8, 'PM', 'Evening', FALSE, 'Evening', FALSE);
INSERT INTO nyc_taxi_dw.dim_time VALUES (21, 21, 9, 'PM', 'Night', FALSE, 'Night', FALSE);
INSERT INTO nyc_taxi_dw.dim_time VALUES (22, 22, 10, 'PM', 'Night', FALSE, 'Night', FALSE);
INSERT INTO nyc_taxi_dw.dim_time VALUES (23, 23, 11, 'PM', 'Night', FALSE, 'Night', FALSE);

-- ============================================================================
-- STEP 8: GENERATE DATE DIMENSION (2020-2030)
-- ============================================================================

INSERT INTO nyc_taxi_dw.dim_date
SELECT
    CAST(TO_CHAR(datum, 'YYYYMMDD') AS INTEGER) AS date_sk,
    datum AS full_date,
    EXTRACT(DOW FROM datum) AS day_of_week,
    TO_CHAR(datum, 'Day') AS day_of_week_name,
    EXTRACT(DAY FROM datum) AS day_of_month,
    EXTRACT(DOY FROM datum) AS day_of_year,
    EXTRACT(WEEK FROM datum) AS week_of_year,
    EXTRACT(MONTH FROM datum) AS month_number,
    TO_CHAR(datum, 'Month') AS month_name,
    TO_CHAR(datum, 'Mon') AS month_short,
    EXTRACT(QUARTER FROM datum) AS quarter,
    'Q' || EXTRACT(QUARTER FROM datum) AS quarter_name,
    EXTRACT(YEAR FROM datum) AS year,
    TO_CHAR(datum, 'YYYY-MM') AS year_month,
    CASE WHEN EXTRACT(DOW FROM datum) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    FALSE AS is_holiday
FROM (
    SELECT 
        DATEADD(day, seq, '2020-01-01'::DATE) AS datum
    FROM (
        SELECT 
            ROW_NUMBER() OVER () - 1 AS seq
        FROM nyc_taxi_dw.stg_yellow_trips
        LIMIT 4018
    )
) dates;

-- ============================================================================
-- STEP 9: LOAD ZONE DIMENSION FROM S3
-- ============================================================================

-- Replace <your-bucket> and <account-id> with actual values
/*
COPY nyc_taxi_dw.dim_zone (location_id, borough, zone_name, service_zone)
FROM 's3://<your-bucket>/raw/zone_lookup/taxi_zone_lookup.csv'
IAM_ROLE 'arn:aws:iam::<account-id>:role/RedshiftS3Role'
CSV
IGNOREHEADER 1
REGION 'us-east-1';
*/

-- After loading, update derived columns
UPDATE nyc_taxi_dw.dim_zone 
SET 
    is_airport = CASE WHEN service_zone = 'Airports' THEN TRUE ELSE FALSE END,
    is_manhattan = CASE WHEN borough = 'Manhattan' THEN TRUE ELSE FALSE END,
    is_yellow_zone = CASE WHEN service_zone = 'Yellow Zone' THEN TRUE ELSE FALSE END,
    zone_category = CASE 
        WHEN service_zone = 'Airports' THEN 'Airport'
        WHEN service_zone = 'Yellow Zone' THEN 'Manhattan Core'
        WHEN borough = 'Manhattan' THEN 'Manhattan Other'
        ELSE 'Outer Borough'
    END;

-- ============================================================================
-- STEP 10: LOAD TRIP DATA FROM S3 (CSV)
-- ============================================================================

-- Replace <your-bucket> and <account-id> with actual values
/*
COPY nyc_taxi_dw.stg_yellow_trips
FROM 's3://<your-bucket>/raw/yellow_tripdata/yellow_tripdata.csv'
IAM_ROLE 'arn:aws:iam::<account-id>:role/RedshiftS3Role'
CSV
IGNOREHEADER 1
DATEFORMAT 'auto'
TIMEFORMAT 'auto'
BLANKSASNULL
EMPTYASNULL
ACCEPTINVCHARS ' '
MAXERROR 1000
REGION 'us-east-1';
*/

-- ============================================================================
-- STEP 11: TRANSFORM STAGING TO FACT TABLE
-- ============================================================================

INSERT INTO nyc_taxi_dw.fact_trip (
    pickup_date_sk,
    pickup_time_sk,
    dropoff_date_sk,
    dropoff_time_sk,
    vendor_sk,
    pickup_zone_sk,
    dropoff_zone_sk,
    ratecode_sk,
    payment_sk,
    store_and_fwd_flag,
    passenger_count,
    trip_distance,
    fare_amount,
    extra_amount,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,
    total_amount,
    trip_duration_minutes,
    tip_percentage,
    is_long_trip,
    is_short_trip,
    is_high_tip,
    is_cash_trip,
    is_airport_trip,
    is_rush_hour
)
SELECT 
    CAST(TO_CHAR(s.tpep_pickup_datetime, 'YYYYMMDD') AS INTEGER) AS pickup_date_sk,
    CAST(EXTRACT(HOUR FROM s.tpep_pickup_datetime) AS SMALLINT) AS pickup_time_sk,
    CAST(TO_CHAR(s.tpep_dropoff_datetime, 'YYYYMMDD') AS INTEGER) AS dropoff_date_sk,
    CAST(EXTRACT(HOUR FROM s.tpep_dropoff_datetime) AS SMALLINT) AS dropoff_time_sk,
    COALESCE(v.vendor_sk, 5) AS vendor_sk,
    COALESCE(pz.zone_sk, 265) AS pickup_zone_sk,
    COALESCE(dz.zone_sk, 265) AS dropoff_zone_sk,
    COALESCE(r.ratecode_sk, 7) AS ratecode_sk,
    COALESCE(p.payment_sk, 5) AS payment_sk,
    s.store_and_fwd_flag,
    CAST(s.passenger_count AS SMALLINT),
    s.trip_distance,
    s.fare_amount,
    s.extra,
    s.mta_tax,
    s.tip_amount,
    s.tolls_amount,
    s.improvement_surcharge,
    s.congestion_surcharge,
    s.airport_fee,
    s.cbd_congestion_fee,
    s.total_amount,
    DATEDIFF(minute, s.tpep_pickup_datetime, s.tpep_dropoff_datetime) AS trip_duration_minutes,
    CASE 
        WHEN s.fare_amount > 0 THEN ROUND((s.tip_amount / s.fare_amount) * 100, 2) 
        ELSE 0 
    END AS tip_percentage,
    CASE WHEN s.trip_distance > 10 THEN TRUE ELSE FALSE END AS is_long_trip,
    CASE WHEN s.trip_distance < 1 THEN TRUE ELSE FALSE END AS is_short_trip,
    CASE WHEN s.fare_amount > 0 AND (s.tip_amount / s.fare_amount) > 0.20 THEN TRUE ELSE FALSE END AS is_high_tip,
    CASE WHEN s.payment_type = 2 THEN TRUE ELSE FALSE END AS is_cash_trip,
    CASE WHEN COALESCE(pz.is_airport, FALSE) OR COALESCE(dz.is_airport, FALSE) THEN TRUE ELSE FALSE END AS is_airport_trip,
    CASE 
        WHEN EXTRACT(HOUR FROM s.tpep_pickup_datetime) BETWEEN 7 AND 9 THEN TRUE
        WHEN EXTRACT(HOUR FROM s.tpep_pickup_datetime) BETWEEN 17 AND 19 THEN TRUE
        ELSE FALSE 
    END AS is_rush_hour
FROM nyc_taxi_dw.stg_yellow_trips s
LEFT JOIN nyc_taxi_dw.dim_vendor v ON s.vendorid = v.vendor_id
LEFT JOIN nyc_taxi_dw.dim_zone pz ON s.pulocationid = pz.location_id
LEFT JOIN nyc_taxi_dw.dim_zone dz ON s.dolocationid = dz.location_id
LEFT JOIN nyc_taxi_dw.dim_ratecode r ON COALESCE(s.ratecodeid, 99) = r.ratecode_id
LEFT JOIN nyc_taxi_dw.dim_payment p ON COALESCE(s.payment_type, 0) = p.payment_type_id
WHERE s.tpep_pickup_datetime IS NOT NULL
  AND s.tpep_dropoff_datetime IS NOT NULL;

-- ============================================================================
-- STEP 12: POPULATE AGGREGATE TABLES
-- ============================================================================

-- Daily Summary
INSERT INTO nyc_taxi_dw.agg_daily_summary (
    date_sk,
    vendor_sk,
    pickup_zone_sk,
    trip_count,
    total_passengers,
    total_distance,
    total_fare,
    total_tips,
    total_revenue,
    avg_trip_distance,
    avg_fare,
    avg_tip_percentage,
    avg_trip_duration,
    cash_trip_count,
    card_trip_count,
    airport_trip_count,
    rush_hour_trip_count
)
SELECT 
    pickup_date_sk,
    vendor_sk,
    pickup_zone_sk,
    COUNT(*) AS trip_count,
    SUM(passenger_count) AS total_passengers,
    SUM(trip_distance) AS total_distance,
    SUM(fare_amount) AS total_fare,
    SUM(tip_amount) AS total_tips,
    SUM(total_amount) AS total_revenue,
    AVG(trip_distance) AS avg_trip_distance,
    AVG(fare_amount) AS avg_fare,
    AVG(tip_percentage) AS avg_tip_percentage,
    AVG(trip_duration_minutes) AS avg_trip_duration,
    SUM(CASE WHEN is_cash_trip THEN 1 ELSE 0 END) AS cash_trip_count,
    SUM(CASE WHEN NOT is_cash_trip THEN 1 ELSE 0 END) AS card_trip_count,
    SUM(CASE WHEN is_airport_trip THEN 1 ELSE 0 END) AS airport_trip_count,
    SUM(CASE WHEN is_rush_hour THEN 1 ELSE 0 END) AS rush_hour_trip_count
FROM nyc_taxi_dw.fact_trip
GROUP BY pickup_date_sk, vendor_sk, pickup_zone_sk;

-- Hourly Zone Summary
INSERT INTO nyc_taxi_dw.agg_hourly_zone (
    date_sk,
    time_sk,
    zone_sk,
    pickup_count,
    dropoff_count,
    net_flow,
    total_fare,
    avg_fare
)
SELECT 
    pickup_date_sk,
    pickup_time_sk,
    pickup_zone_sk,
    COUNT(*) AS pickup_count,
    0 AS dropoff_count,
    0 AS net_flow,
    SUM(fare_amount) AS total_fare,
    AVG(fare_amount) AS avg_fare
FROM nyc_taxi_dw.fact_trip
GROUP BY pickup_date_sk, pickup_time_sk, pickup_zone_sk;

-- ============================================================================
-- STEP 13: ANALYZE TABLES FOR QUERY OPTIMIZATION
-- ============================================================================

ANALYZE nyc_taxi_dw.dim_vendor;
ANALYZE nyc_taxi_dw.dim_zone;
ANALYZE nyc_taxi_dw.dim_ratecode;
ANALYZE nyc_taxi_dw.dim_payment;
ANALYZE nyc_taxi_dw.dim_date;
ANALYZE nyc_taxi_dw.dim_time;
ANALYZE nyc_taxi_dw.fact_trip;
ANALYZE nyc_taxi_dw.agg_daily_summary;
ANALYZE nyc_taxi_dw.agg_hourly_zone;

-- ============================================================================
-- STEP 14: VERIFY LOAD
-- ============================================================================

SELECT 'dim_vendor' AS table_name, COUNT(*) AS row_count FROM nyc_taxi_dw.dim_vendor
UNION ALL
SELECT 'dim_zone', COUNT(*) FROM nyc_taxi_dw.dim_zone
UNION ALL
SELECT 'dim_ratecode', COUNT(*) FROM nyc_taxi_dw.dim_ratecode
UNION ALL
SELECT 'dim_payment', COUNT(*) FROM nyc_taxi_dw.dim_payment
UNION ALL
SELECT 'dim_date', COUNT(*) FROM nyc_taxi_dw.dim_date
UNION ALL
SELECT 'dim_time', COUNT(*) FROM nyc_taxi_dw.dim_time
UNION ALL
SELECT 'stg_yellow_trips', COUNT(*) FROM nyc_taxi_dw.stg_yellow_trips
UNION ALL
SELECT 'fact_trip', COUNT(*) FROM nyc_taxi_dw.fact_trip
UNION ALL
SELECT 'agg_daily_summary', COUNT(*) FROM nyc_taxi_dw.agg_daily_summary
UNION ALL
SELECT 'agg_hourly_zone', COUNT(*) FROM nyc_taxi_dw.agg_hourly_zone;

-- ============================================================================
-- STEP 15: SAMPLE QUERIES
-- ============================================================================

-- Daily revenue by vendor
SELECT 
    d.full_date,
    v.vendor_name,
    COUNT(*) AS trips,
    SUM(f.total_amount) AS revenue
FROM nyc_taxi_dw.fact_trip f
JOIN nyc_taxi_dw.dim_date d ON f.pickup_date_sk = d.date_sk
JOIN nyc_taxi_dw.dim_vendor v ON f.vendor_sk = v.vendor_sk
GROUP BY d.full_date, v.vendor_name
ORDER BY d.full_date, revenue DESC;

-- Top 10 busiest zones
SELECT 
    z.zone_name,
    z.borough,
    COUNT(*) AS trips,
    SUM(f.total_amount) AS revenue
FROM nyc_taxi_dw.fact_trip f
JOIN nyc_taxi_dw.dim_zone z ON f.pickup_zone_sk = z.zone_sk
GROUP BY z.zone_name, z.borough
ORDER BY trips DESC
LIMIT 10;

-- Rush hour vs non-rush hour comparison
SELECT 
    CASE WHEN f.is_rush_hour THEN 'Rush Hour' ELSE 'Non-Rush Hour' END AS period,
    COUNT(*) AS trips,
    AVG(f.fare_amount) AS avg_fare,
    AVG(f.tip_percentage) AS avg_tip_pct
FROM nyc_taxi_dw.fact_trip f
GROUP BY CASE WHEN f.is_rush_hour THEN 'Rush Hour' ELSE 'Non-Rush Hour' END;
