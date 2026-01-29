-- ============================================================================
-- SCD TYPE 2 IMPLEMENTATION FOR NYC TAXI DATA WAREHOUSE
-- ============================================================================
-- Amazon Redshift Compatible
-- Implements SCD Type 2 for Vendor and Zone dimensions
-- ============================================================================

-- ============================================================================
-- SECTION 1: SCD TYPE 2 DIMENSION TABLES
-- ============================================================================

-- ---------------------------------------------------------------------------
-- DIM_VENDOR_SCD2 - Slowly Changing Dimension Type 2
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS nyc_taxi_dw.dim_vendor_scd2 CASCADE;

CREATE TABLE nyc_taxi_dw.dim_vendor_scd2 (
    -- Surrogate Key
    vendor_sk               INTEGER IDENTITY(1,1),
    
    -- Natural/Business Key
    vendor_id               SMALLINT NOT NULL,
    
    -- Dimension Attributes
    vendor_name             VARCHAR(100) DEFAULT 'Unknown',
    vendor_code             VARCHAR(10) DEFAULT 'UNK',
    vendor_status           VARCHAR(20) DEFAULT 'ACTIVE',
    contact_email           VARCHAR(100),
    contact_phone           VARCHAR(20),
    
    -- SCD Type 2 Tracking Columns
    effective_start_date    TIMESTAMP NOT NULL DEFAULT SYSDATE,
    effective_end_date      TIMESTAMP NOT NULL DEFAULT '9999-12-31 23:59:59',
    is_current              BOOLEAN NOT NULL DEFAULT TRUE,
    version_number          INTEGER NOT NULL DEFAULT 1,
    
    -- SCD Type 3 Column (Previous Value)
    previous_vendor_name    VARCHAR(100),
    
    -- Audit Columns
    created_at              TIMESTAMP DEFAULT SYSDATE,
    created_by              VARCHAR(50) DEFAULT 'SYSTEM',
    updated_at              TIMESTAMP DEFAULT SYSDATE,
    updated_by              VARCHAR(50) DEFAULT 'SYSTEM',
    change_reason           VARCHAR(200),
    
    -- Row Hash for Change Detection
    row_hash                VARCHAR(64),
    
    PRIMARY KEY (vendor_sk)
)
DISTSTYLE ALL
SORTKEY (vendor_id, is_current, effective_start_date);

-- ---------------------------------------------------------------------------
-- DIM_ZONE_SCD2 - Slowly Changing Dimension Type 2
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS nyc_taxi_dw.dim_zone_scd2 CASCADE;

CREATE TABLE nyc_taxi_dw.dim_zone_scd2 (
    -- Surrogate Key
    zone_sk                 INTEGER IDENTITY(1,1),
    
    -- Natural/Business Key
    location_id             SMALLINT NOT NULL,
    
    -- Dimension Attributes
    borough                 VARCHAR(50) DEFAULT 'Unknown',
    zone_name               VARCHAR(100) DEFAULT 'Unknown',
    service_zone            VARCHAR(50) DEFAULT 'Unknown',
    
    -- Derived Attributes
    is_airport              BOOLEAN DEFAULT FALSE,
    is_manhattan            BOOLEAN DEFAULT FALSE,
    is_yellow_zone          BOOLEAN DEFAULT FALSE,
    zone_category           VARCHAR(30) DEFAULT 'Other',
    
    -- SCD Type 2 Tracking Columns
    effective_start_date    TIMESTAMP NOT NULL DEFAULT SYSDATE,
    effective_end_date      TIMESTAMP NOT NULL DEFAULT '9999-12-31 23:59:59',
    is_current              BOOLEAN NOT NULL DEFAULT TRUE,
    version_number          INTEGER NOT NULL DEFAULT 1,
    
    -- SCD Type 3 Column (Previous Value)
    previous_zone_name      VARCHAR(100),
    
    -- Audit Columns
    created_at              TIMESTAMP DEFAULT SYSDATE,
    created_by              VARCHAR(50) DEFAULT 'SYSTEM',
    updated_at              TIMESTAMP DEFAULT SYSDATE,
    updated_by              VARCHAR(50) DEFAULT 'SYSTEM',
    change_reason           VARCHAR(200),
    
    -- Row Hash for Change Detection
    row_hash                VARCHAR(64),
    
    PRIMARY KEY (zone_sk)
)
DISTSTYLE ALL
SORTKEY (location_id, is_current, effective_start_date);

-- ============================================================================
-- SECTION 2: SEED INITIAL DATA INTO SCD2 TABLES
-- ============================================================================

-- ---------------------------------------------------------------------------
-- Load Vendor SCD2 from existing dim_vendor
-- ---------------------------------------------------------------------------
INSERT INTO nyc_taxi_dw.dim_vendor_scd2 (
    vendor_id,
    vendor_name,
    vendor_code,
    vendor_status,
    effective_start_date,
    effective_end_date,
    is_current,
    version_number,
    created_by,
    change_reason,
    row_hash
)
SELECT 
    vendor_id,
    vendor_name,
    vendor_code,
    'ACTIVE',
    SYSDATE,
    '9999-12-31 23:59:59',
    TRUE,
    1,
    'INITIAL_LOAD',
    'Initial SCD2 migration',
    MD5(vendor_id || '|' || vendor_name || '|' || vendor_code)
FROM nyc_taxi_dw.dim_vendor;

-- ---------------------------------------------------------------------------
-- Load Zone SCD2 from existing dim_zone
-- ---------------------------------------------------------------------------
INSERT INTO nyc_taxi_dw.dim_zone_scd2 (
    location_id,
    borough,
    zone_name,
    service_zone,
    is_airport,
    is_manhattan,
    is_yellow_zone,
    zone_category,
    effective_start_date,
    effective_end_date,
    is_current,
    version_number,
    created_by,
    change_reason,
    row_hash
)
SELECT 
    location_id,
    borough,
    zone_name,
    service_zone,
    is_airport,
    is_manhattan,
    is_yellow_zone,
    zone_category,
    SYSDATE,
    '9999-12-31 23:59:59',
    TRUE,
    1,
    'INITIAL_LOAD',
    'Initial SCD2 migration',
    MD5(location_id || '|' || zone_name || '|' || borough || '|' || service_zone)
FROM nyc_taxi_dw.dim_zone;

-- ============================================================================
-- SECTION 3: SCD TYPE 2 MERGE PROCEDURES
-- ============================================================================

-- ---------------------------------------------------------------------------
-- Staging table for Vendor updates
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS nyc_taxi_dw.stg_vendor_updates;

CREATE TABLE nyc_taxi_dw.stg_vendor_updates (
    vendor_id               SMALLINT NOT NULL,
    vendor_name             VARCHAR(100),
    vendor_code             VARCHAR(10),
    vendor_status           VARCHAR(20),
    contact_email           VARCHAR(100),
    contact_phone           VARCHAR(20),
    updated_by              VARCHAR(50),
    change_reason           VARCHAR(200)
);

-- ---------------------------------------------------------------------------
-- SCD Type 2 Merge for Vendor - Step by Step Process
-- ---------------------------------------------------------------------------

-- STEP 1: Insert sample update to staging (for testing)
/*
INSERT INTO nyc_taxi_dw.stg_vendor_updates VALUES
(2, 'Curb Mobility LLC - Updated', 'CURB', 'ACTIVE', 'contact@curb.com', '555-1234', 'admin', 'Name correction');
*/

-- STEP 2: Expire current records that have changes
UPDATE nyc_taxi_dw.dim_vendor_scd2
SET 
    effective_end_date = DATEADD(second, -1, SYSDATE),
    is_current = FALSE,
    updated_at = SYSDATE,
    updated_by = 'SCD2_PROCESS'
WHERE vendor_id IN (
    SELECT s.vendor_id 
    FROM nyc_taxi_dw.stg_vendor_updates s
    JOIN nyc_taxi_dw.dim_vendor_scd2 d 
        ON s.vendor_id = d.vendor_id 
        AND d.is_current = TRUE
    WHERE MD5(s.vendor_id || '|' || s.vendor_name || '|' || s.vendor_code) 
          != d.row_hash
)
AND is_current = TRUE;

-- STEP 3: Insert new versions for changed records
INSERT INTO nyc_taxi_dw.dim_vendor_scd2 (
    vendor_id,
    vendor_name,
    vendor_code,
    vendor_status,
    contact_email,
    contact_phone,
    effective_start_date,
    effective_end_date,
    is_current,
    version_number,
    previous_vendor_name,
    created_by,
    change_reason,
    row_hash
)
SELECT 
    s.vendor_id,
    s.vendor_name,
    s.vendor_code,
    s.vendor_status,
    s.contact_email,
    s.contact_phone,
    SYSDATE,
    '9999-12-31 23:59:59',
    TRUE,
    COALESCE(d.version_number, 0) + 1,
    d.vendor_name,  -- Store previous name
    s.updated_by,
    s.change_reason,
    MD5(s.vendor_id || '|' || s.vendor_name || '|' || s.vendor_code)
FROM nyc_taxi_dw.stg_vendor_updates s
LEFT JOIN nyc_taxi_dw.dim_vendor_scd2 d 
    ON s.vendor_id = d.vendor_id 
    AND d.is_current = FALSE
    AND d.effective_end_date >= DATEADD(second, -5, SYSDATE);

-- STEP 4: Insert new records (not existing in dimension)
INSERT INTO nyc_taxi_dw.dim_vendor_scd2 (
    vendor_id,
    vendor_name,
    vendor_code,
    vendor_status,
    contact_email,
    contact_phone,
    effective_start_date,
    effective_end_date,
    is_current,
    version_number,
    created_by,
    change_reason,
    row_hash
)
SELECT 
    s.vendor_id,
    s.vendor_name,
    s.vendor_code,
    s.vendor_status,
    s.contact_email,
    s.contact_phone,
    SYSDATE,
    '9999-12-31 23:59:59',
    TRUE,
    1,
    s.updated_by,
    'New vendor added',
    MD5(s.vendor_id || '|' || s.vendor_name || '|' || s.vendor_code)
FROM nyc_taxi_dw.stg_vendor_updates s
WHERE NOT EXISTS (
    SELECT 1 FROM nyc_taxi_dw.dim_vendor_scd2 d 
    WHERE d.vendor_id = s.vendor_id
);

-- STEP 5: Clear staging table
TRUNCATE TABLE nyc_taxi_dw.stg_vendor_updates;

-- ============================================================================
-- SECTION 4: SCD TYPE 2 QUERY PATTERNS
-- ============================================================================

-- ---------------------------------------------------------------------------
-- Get Current Records Only
-- ---------------------------------------------------------------------------
SELECT * 
FROM nyc_taxi_dw.dim_vendor_scd2 
WHERE is_current = TRUE;

-- ---------------------------------------------------------------------------
-- Point-in-Time Query (What was vendor name on specific date?)
-- ---------------------------------------------------------------------------
SELECT *
FROM nyc_taxi_dw.dim_vendor_scd2
WHERE vendor_id = 2
  AND '2025-08-15 10:00:00' >= effective_start_date
  AND '2025-08-15 10:00:00' < effective_end_date;

-- ---------------------------------------------------------------------------
-- Get Full History for a Vendor
-- ---------------------------------------------------------------------------
SELECT 
    vendor_id,
    vendor_name,
    previous_vendor_name,
    version_number,
    effective_start_date,
    effective_end_date,
    is_current,
    change_reason
FROM nyc_taxi_dw.dim_vendor_scd2
WHERE vendor_id = 2
ORDER BY version_number;

-- ---------------------------------------------------------------------------
-- Audit Trail Query
-- ---------------------------------------------------------------------------
SELECT 
    vendor_id,
    vendor_name,
    version_number,
    effective_start_date,
    effective_end_date,
    DATEDIFF(day, effective_start_date, 
        CASE WHEN effective_end_date = '9999-12-31 23:59:59' 
             THEN SYSDATE 
             ELSE effective_end_date END) AS days_active,
    change_reason,
    created_by
FROM nyc_taxi_dw.dim_vendor_scd2
ORDER BY vendor_id, version_number;

-- ============================================================================
-- SECTION 5: UPDATED FACT TABLE WITH SCD2 REFERENCES
-- ============================================================================

-- ---------------------------------------------------------------------------
-- Fact table now references SCD2 surrogate keys
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS nyc_taxi_dw.fact_trip_scd2 CASCADE;

CREATE TABLE nyc_taxi_dw.fact_trip_scd2 (
    trip_sk                 BIGINT IDENTITY(1,1),
    
    -- SCD2 Dimension Keys
    pickup_date_sk          INTEGER NOT NULL,
    pickup_time_sk          SMALLINT NOT NULL,
    dropoff_date_sk         INTEGER NOT NULL,
    dropoff_time_sk         SMALLINT NOT NULL,
    vendor_sk               INTEGER NOT NULL,           -- References dim_vendor_scd2
    pickup_zone_sk          INTEGER NOT NULL,           -- References dim_zone_scd2
    dropoff_zone_sk         INTEGER NOT NULL,           -- References dim_zone_scd2
    ratecode_sk             SMALLINT NOT NULL,
    payment_sk              SMALLINT NOT NULL,
    
    -- Degenerate Dimension
    store_and_fwd_flag      VARCHAR(1),
    
    -- Measures
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
    
    -- Flags
    is_long_trip            BOOLEAN DEFAULT FALSE,
    is_short_trip           BOOLEAN DEFAULT FALSE,
    is_high_tip             BOOLEAN DEFAULT FALSE,
    is_cash_trip            BOOLEAN DEFAULT FALSE,
    is_airport_trip         BOOLEAN DEFAULT FALSE,
    is_rush_hour            BOOLEAN DEFAULT FALSE,
    
    -- Audit
    etl_load_timestamp      TIMESTAMP DEFAULT SYSDATE,
    
    PRIMARY KEY (trip_sk)
)
DISTSTYLE KEY
DISTKEY (pickup_date_sk)
COMPOUND SORTKEY (pickup_date_sk, pickup_time_sk, vendor_sk);

-- ---------------------------------------------------------------------------
-- Load Fact Table with SCD2 Lookups
-- ---------------------------------------------------------------------------
INSERT INTO nyc_taxi_dw.fact_trip_scd2 (
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
    CAST(TO_CHAR(s.tpep_pickup_datetime, 'YYYYMMDD') AS INTEGER),
    CAST(EXTRACT(HOUR FROM s.tpep_pickup_datetime) AS SMALLINT),
    CAST(TO_CHAR(s.tpep_dropoff_datetime, 'YYYYMMDD') AS INTEGER),
    CAST(EXTRACT(HOUR FROM s.tpep_dropoff_datetime) AS SMALLINT),
    
    -- SCD2 Lookup: Get surrogate key valid at trip time
    COALESCE(v.vendor_sk, 1),
    COALESCE(pz.zone_sk, 1),
    COALESCE(dz.zone_sk, 1),
    
    COALESCE(r.ratecode_sk, 7),
    COALESCE(p.payment_sk, 5),
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
    DATEDIFF(minute, s.tpep_pickup_datetime, s.tpep_dropoff_datetime),
    CASE WHEN s.fare_amount > 0 THEN ROUND((s.tip_amount / s.fare_amount) * 100, 2) ELSE 0 END,
    CASE WHEN s.trip_distance > 10 THEN TRUE ELSE FALSE END,
    CASE WHEN s.trip_distance < 1 THEN TRUE ELSE FALSE END,
    CASE WHEN s.fare_amount > 0 AND (s.tip_amount / s.fare_amount) > 0.20 THEN TRUE ELSE FALSE END,
    CASE WHEN s.payment_type = 2 THEN TRUE ELSE FALSE END,
    CASE WHEN COALESCE(pz.is_airport, FALSE) OR COALESCE(dz.is_airport, FALSE) THEN TRUE ELSE FALSE END,
    CASE WHEN EXTRACT(HOUR FROM s.tpep_pickup_datetime) BETWEEN 7 AND 9 
           OR EXTRACT(HOUR FROM s.tpep_pickup_datetime) BETWEEN 17 AND 19 
         THEN TRUE ELSE FALSE END
         
FROM nyc_taxi_dw.stg_yellow_trips s

-- SCD2 Join: Match vendor valid at pickup time
LEFT JOIN nyc_taxi_dw.dim_vendor_scd2 v 
    ON s.vendorid = v.vendor_id
    AND s.tpep_pickup_datetime >= v.effective_start_date
    AND s.tpep_pickup_datetime < v.effective_end_date

-- SCD2 Join: Match pickup zone valid at pickup time
LEFT JOIN nyc_taxi_dw.dim_zone_scd2 pz 
    ON s.pulocationid = pz.location_id
    AND s.tpep_pickup_datetime >= pz.effective_start_date
    AND s.tpep_pickup_datetime < pz.effective_end_date

-- SCD2 Join: Match dropoff zone valid at dropoff time
LEFT JOIN nyc_taxi_dw.dim_zone_scd2 dz 
    ON s.dolocationid = dz.location_id
    AND s.tpep_dropoff_datetime >= dz.effective_start_date
    AND s.tpep_dropoff_datetime < dz.effective_end_date

LEFT JOIN nyc_taxi_dw.dim_ratecode r ON COALESCE(s.ratecodeid, 99) = r.ratecode_id
LEFT JOIN nyc_taxi_dw.dim_payment p ON COALESCE(s.payment_type, 0) = p.payment_type_id

WHERE s.tpep_pickup_datetime IS NOT NULL
  AND s.tpep_dropoff_datetime IS NOT NULL;

-- ============================================================================
-- SECTION 6: VIEWS FOR QUICKSIGHT
-- ============================================================================

-- ---------------------------------------------------------------------------
-- View: Current Vendor Dimension
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW nyc_taxi_dw.v_dim_vendor_current AS
SELECT 
    vendor_sk,
    vendor_id,
    vendor_name,
    vendor_code,
    vendor_status,
    contact_email,
    contact_phone
FROM nyc_taxi_dw.dim_vendor_scd2
WHERE is_current = TRUE;

-- ---------------------------------------------------------------------------
-- View: Current Zone Dimension
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW nyc_taxi_dw.v_dim_zone_current AS
SELECT 
    zone_sk,
    location_id,
    borough,
    zone_name,
    service_zone,
    is_airport,
    is_manhattan,
    is_yellow_zone,
    zone_category
FROM nyc_taxi_dw.dim_zone_scd2
WHERE is_current = TRUE;

-- ---------------------------------------------------------------------------
-- View: Daily Trip Summary (For QuickSight Dashboard)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW nyc_taxi_dw.v_daily_trip_summary AS
SELECT 
    d.full_date AS trip_date,
    d.day_of_week_name,
    d.month_name,
    d.year,
    d.is_weekend,
    v.vendor_name,
    COUNT(*) AS total_trips,
    SUM(f.passenger_count) AS total_passengers,
    ROUND(SUM(f.trip_distance), 2) AS total_distance,
    ROUND(SUM(f.fare_amount), 2) AS total_fare,
    ROUND(SUM(f.tip_amount), 2) AS total_tips,
    ROUND(SUM(f.total_amount), 2) AS total_revenue,
    ROUND(AVG(f.trip_distance), 2) AS avg_trip_distance,
    ROUND(AVG(f.fare_amount), 2) AS avg_fare,
    ROUND(AVG(f.tip_percentage), 2) AS avg_tip_percentage,
    ROUND(AVG(f.trip_duration_minutes), 2) AS avg_trip_duration,
    SUM(CASE WHEN f.is_cash_trip THEN 1 ELSE 0 END) AS cash_trips,
    SUM(CASE WHEN f.is_airport_trip THEN 1 ELSE 0 END) AS airport_trips,
    SUM(CASE WHEN f.is_rush_hour THEN 1 ELSE 0 END) AS rush_hour_trips
FROM nyc_taxi_dw.fact_trip_scd2 f
JOIN nyc_taxi_dw.dim_date d ON f.pickup_date_sk = d.date_sk
JOIN nyc_taxi_dw.dim_vendor_scd2 v ON f.vendor_sk = v.vendor_sk
GROUP BY d.full_date, d.day_of_week_name, d.month_name, d.year, d.is_weekend, v.vendor_name;

-- ---------------------------------------------------------------------------
-- View: Zone Performance (For QuickSight Dashboard)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW nyc_taxi_dw.v_zone_performance AS
SELECT 
    z.borough,
    z.zone_name,
    z.zone_category,
    z.is_airport,
    COUNT(*) AS pickup_count,
    ROUND(SUM(f.total_amount), 2) AS total_revenue,
    ROUND(AVG(f.fare_amount), 2) AS avg_fare,
    ROUND(AVG(f.tip_percentage), 2) AS avg_tip_pct,
    ROUND(AVG(f.trip_distance), 2) AS avg_distance
FROM nyc_taxi_dw.fact_trip_scd2 f
JOIN nyc_taxi_dw.dim_zone_scd2 z ON f.pickup_zone_sk = z.zone_sk
GROUP BY z.borough, z.zone_name, z.zone_category, z.is_airport;

-- ---------------------------------------------------------------------------
-- View: Hourly Demand Pattern (For QuickSight Dashboard)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW nyc_taxi_dw.v_hourly_demand AS
SELECT 
    t.hour_24,
    t.time_period,
    t.rush_hour_flag,
    t.day_part,
    d.day_of_week_name,
    d.is_weekend,
    COUNT(*) AS trip_count,
    ROUND(SUM(f.total_amount), 2) AS total_revenue,
    ROUND(AVG(f.fare_amount), 2) AS avg_fare
FROM nyc_taxi_dw.fact_trip_scd2 f
JOIN nyc_taxi_dw.dim_time t ON f.pickup_time_sk = t.time_sk
JOIN nyc_taxi_dw.dim_date d ON f.pickup_date_sk = d.date_sk
GROUP BY t.hour_24, t.time_period, t.rush_hour_flag, t.day_part, d.day_of_week_name, d.is_weekend;

-- ---------------------------------------------------------------------------
-- View: Payment Analysis (For QuickSight Dashboard)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW nyc_taxi_dw.v_payment_analysis AS
SELECT 
    p.payment_type_name,
    p.payment_category,
    COUNT(*) AS trip_count,
    ROUND(SUM(f.total_amount), 2) AS total_revenue,
    ROUND(AVG(f.fare_amount), 2) AS avg_fare,
    ROUND(AVG(f.tip_amount), 2) AS avg_tip,
    ROUND(AVG(f.tip_percentage), 2) AS avg_tip_pct
FROM nyc_taxi_dw.fact_trip_scd2 f
JOIN nyc_taxi_dw.dim_payment p ON f.payment_sk = p.payment_sk
GROUP BY p.payment_type_name, p.payment_category;

-- ---------------------------------------------------------------------------
-- View: SCD2 Change History (For Audit Dashboard)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW nyc_taxi_dw.v_vendor_history AS
SELECT 
    vendor_id,
    vendor_name,
    previous_vendor_name,
    vendor_status,
    version_number,
    effective_start_date,
    effective_end_date,
    is_current,
    change_reason,
    created_by,
    created_at
FROM nyc_taxi_dw.dim_vendor_scd2
ORDER BY vendor_id, version_number;

-- ============================================================================
-- SECTION 7: VERIFY SETUP
-- ============================================================================

-- Check row counts
SELECT 'dim_vendor_scd2' AS table_name, COUNT(*) AS rows FROM nyc_taxi_dw.dim_vendor_scd2
UNION ALL SELECT 'dim_zone_scd2', COUNT(*) FROM nyc_taxi_dw.dim_zone_scd2
UNION ALL SELECT 'fact_trip_scd2', COUNT(*) FROM nyc_taxi_dw.fact_trip_scd2;

-- Check current records
SELECT 'Vendor Current' AS check_type, COUNT(*) AS cnt 
FROM nyc_taxi_dw.dim_vendor_scd2 WHERE is_current = TRUE
UNION ALL
SELECT 'Zone Current', COUNT(*) 
FROM nyc_taxi_dw.dim_zone_scd2 WHERE is_current = TRUE;

-- Analyze tables for performance
ANALYZE nyc_taxi_dw.dim_vendor_scd2;
ANALYZE nyc_taxi_dw.dim_zone_scd2;
ANALYZE nyc_taxi_dw.fact_trip_scd2;
