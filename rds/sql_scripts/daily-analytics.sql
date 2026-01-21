ALTER TABLE mdm_golden_trip_level
ADD COLUMN pickup_dt DATETIME
    GENERATED ALWAYS AS (STR_TO_DATE(tpep_pickup_datetime, '%Y-%m-%d %H:%i:%s')) STORED;

CREATE INDEX idx_pickup_dt ON mdm_golden_trip_level(pickup_dt);


SELECT
    'PRE_CHECK_1.1_DATA_FRESHNESS' AS check_name,
    COUNT(*) AS total_records,
    COUNT(pickup_dt) AS valid_datetime_count,
    (SELECT pickup_dt 
     FROM mdm_golden_trip_level
     WHERE data_zone = 'curated_trip_level'
     ORDER BY pickup_dt DESC
     LIMIT 1) AS latest_valid_trip
FROM mdm_golden_trip_level
WHERE data_zone = 'curated_trip_level';

SELECT
    'PRE_CHECK_1.2_DATETIME_FORMAT_VALIDATION' AS check_name,
    COUNT(*) AS total_records,
    
    -- Count of NULL pickup datetime
    SUM(CASE WHEN pickup_dt IS NULL THEN 1 ELSE 0 END) AS null_pickup_datetime,
    
    -- No invalid format column needed since pickup_dt is proper DATETIME
    0 AS invalid_pickup_format,
    
    -- Status based on null pickup_dt
    CASE
        WHEN SUM(CASE WHEN pickup_dt IS NULL THEN 1 ELSE 0 END) > 100 
            THEN 'FAIL: Too many invalid datetime values'
        WHEN SUM(CASE WHEN pickup_dt IS NULL THEN 1 ELSE 0 END) > 0 
            THEN 'WARNING: Some invalid datetime values detected'
        ELSE 'PASS'
    END AS status

FROM mdm_golden_trip_level
WHERE data_zone = 'curated_trip_level';


SELECT 
    'PRE_CHECK_1.3_CRITICAL_FIELDS_VALIDATION' AS check_name,
    COUNT(*) AS total_records,
    
    -- Null checks for critical fields
    SUM(CASE WHEN trip_id IS NULL THEN 1 ELSE 0 END) AS null_trip_id,
    SUM(CASE WHEN vendorid IS NULL THEN 1 ELSE 0 END) AS null_vendor,
    SUM(CASE WHEN total_amount IS NULL THEN 1 ELSE 0 END) AS null_total_amount,
    SUM(CASE WHEN fare_amount IS NULL THEN 1 ELSE 0 END) AS null_fare_amount,
    SUM(CASE WHEN trip_distance IS NULL THEN 1 ELSE 0 END) AS null_trip_distance,
    SUM(CASE WHEN pulocationid IS NULL THEN 1 ELSE 0 END) AS null_pickup_location,
    
    -- Invalid format counts are now always 0 because columns are properly typed
    0 AS invalid_vendor_format,
    0 AS invalid_amount_format,
    0 AS invalid_pickup_location_format,
    
    -- Overall status
    CASE 
        WHEN SUM(
            CASE 
                WHEN trip_id IS NULL
                  OR vendorid IS NULL
                  OR total_amount IS NULL
                THEN 1 ELSE 0 
            END
        ) > 0 
            THEN CONCAT(
                'FAIL: ',
                SUM(
                    CASE 
                        WHEN trip_id IS NULL
                          OR vendorid IS NULL
                          OR total_amount IS NULL
                        THEN 1 ELSE 0 
                    END
                ),
                ' records with NULL critical fields'
            )
        ELSE 'PASS'
    END AS status

FROM mdm_golden_trip_level
WHERE pickup_dt >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
  AND data_zone = 'curated_trip_level';
  

SELECT 
    'PRE_CHECK_1.4_NEGATIVE_AMOUNTS' AS check_name,
    COUNT(*) AS total_violations,
    SUM(CASE 
        WHEN total_amount IS NOT NULL 
         AND total_amount REGEXP '^-?[0-9]+\.?[0-9]*$'
         AND CAST(total_amount AS DECIMAL(10,2)) < 0 
        THEN 1 ELSE 0 
    END) AS negative_total,
    SUM(CASE 
        WHEN fare_amount IS NOT NULL 
         AND fare_amount REGEXP '^-?[0-9]+\.?[0-9]*$'
         AND CAST(fare_amount AS DECIMAL(10,2)) < 0 
        THEN 1 ELSE 0 
    END) AS negative_fare,
    SUM(CASE 
        WHEN trip_distance IS NOT NULL 
         AND trip_distance REGEXP '^-?[0-9]+\.?[0-9]*$'
         AND CAST(trip_distance AS DECIMAL(10,2)) < 0 
        THEN 1 ELSE 0 
    END) AS negative_distance,
    CASE 
        WHEN COUNT(*) > 100 THEN CONCAT('FAIL: ', COUNT(*), ' records with negative amounts')
        WHEN COUNT(*) > 0 THEN CONCAT('WARNING: ', COUNT(*), ' records with negative amounts')
        ELSE 'PASS'
    END AS status
FROM mdm_golden_trip_level
WHERE (
    (total_amount IS NOT NULL AND total_amount REGEXP '^-?[0-9]+\.?[0-9]*$' AND CAST(total_amount AS DECIMAL(10,2)) < 0) 
    OR (fare_amount IS NOT NULL AND fare_amount REGEXP '^-?[0-9]+\.?[0-9]*$' AND CAST(fare_amount AS DECIMAL(10,2)) < 0)
    OR (trip_distance IS NOT NULL AND trip_distance REGEXP '^-?[0-9]+\.?[0-9]*$' AND CAST(trip_distance AS DECIMAL(10,2)) < 0)
)
  AND tpep_pickup_datetime >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
  AND data_zone = 'curated_trip_level';

-- Check 1.5: Temporal consistency with proper datetime handling
SELECT 
    'PRE_CHECK_1.5_TEMPORAL_CONSISTENCY' AS check_name,
    COUNT(*) AS invalid_time_records,
    CASE 
        WHEN COUNT(*) > 100 THEN CONCAT('FAIL: ', COUNT(*), ' trips with invalid time order')
        WHEN COUNT(*) > 0 THEN CONCAT('WARNING: ', COUNT(*), ' trips with invalid time order')
        ELSE 'PASS'
    END AS status
FROM mdm_golden_trip_level
WHERE tpep_pickup_datetime IS NOT NULL
  AND tpep_dropoff_datetime IS NOT NULL
  AND tpep_pickup_datetime REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$'
  AND tpep_dropoff_datetime REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$'
  AND tpep_pickup_datetime >= tpep_dropoff_datetime
  AND tpep_pickup_datetime >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
  AND data_zone = 'curated_trip_level';
  
  SELECT 
    'PRE_CHECK_1.6_MASTER_DATA_ACTIVE' AS check_name,
    (SELECT COUNT(*) FROM mdm_vendor WHERE lifecycle_state = 'ACTIVE') AS active_vendors,
    (SELECT COUNT(*) FROM mdm_zone WHERE lifecycle_state = 'ACTIVE') AS active_zones,
    (SELECT COUNT(*) FROM mdm_ratecode WHERE lifecycle_state = 'ACTIVE') AS active_ratecodes,
    CASE 
        WHEN (SELECT COUNT(*) FROM mdm_vendor WHERE lifecycle_state = 'ACTIVE') = 0 
            THEN 'FAIL: No active vendors'
        WHEN (SELECT COUNT(*) FROM mdm_zone WHERE lifecycle_state = 'ACTIVE') = 0 
            THEN 'FAIL: No active zones'
        WHEN (SELECT COUNT(*) FROM mdm_ratecode WHERE lifecycle_state = 'ACTIVE') = 0 
            THEN 'FAIL: No active ratecodes'
        ELSE 'PASS'
    END AS status;
    

DROP TABLE IF EXISTS analytics_daily_trip_summary;

CREATE TABLE analytics_daily_trip_summary (

    -- Primary Key
    summary_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT 'Surrogate key',

    -- Dimension Keys
    summary_date DATE NOT NULL COMMENT 'Trip date derived from pickup_dt',
    vendor_id INT NOT NULL COMMENT 'Source vendor ID from trip data',
    vendor_name VARCHAR(200) COMMENT 'Vendor business name from MDM',

    pickup_zone_id INT NOT NULL COMMENT 'Pickup location ID',
    pickup_zone_name VARCHAR(200) COMMENT 'Pickup zone name',
    pickup_borough VARCHAR(100) COMMENT 'Pickup borough (Manhattan, Queens, etc)',

    dropoff_zone_id INT COMMENT 'Dropoff location ID',
    dropoff_zone_name VARCHAR(200) COMMENT 'Dropoff zone name',
    dropoff_borough VARCHAR(100) COMMENT 'Dropoff borough',

    -- Trip Volume Metrics
    total_trips INT NOT NULL DEFAULT 0 COMMENT 'Count of trips',
    total_passengers INT COMMENT 'Sum of passenger counts',

    -- Revenue Metrics (USD)
    total_revenue DECIMAL(15,2) NOT NULL DEFAULT 0.00 COMMENT 'Sum of total_amount',
    total_fare DECIMAL(15,2) COMMENT 'Sum of fare_amount',
    total_tips DECIMAL(15,2) COMMENT 'Sum of tip_amount',
    total_tolls DECIMAL(15,2) COMMENT 'Sum of tolls_amount',
    total_surcharges DECIMAL(15,2) COMMENT 'Sum of congestion + airport fees',

    -- Distance Metrics
    total_distance DECIMAL(15,2) COMMENT 'Sum of trip_distance in miles',
    avg_distance DECIMAL(10,2) COMMENT 'Average trip distance',

    -- Duration Metrics (minutes)
    avg_trip_duration DECIMAL(10,2) COMMENT 'Average trip duration in minutes',

    -- Financial Averages
    avg_fare DECIMAL(10,2) COMMENT 'Average fare per trip',
    avg_tip DECIMAL(10,2) COMMENT 'Average tip per trip',
    avg_total_amount DECIMAL(10,2) COMMENT 'Average total amount per trip',

    -- Tip Analysis
    trips_with_tips INT COMMENT 'Count of trips with tip > 0',
    tip_percentage DECIMAL(5,2) COMMENT 'Percentage of trips with tips',
    avg_tip_pct DECIMAL(5,2) COMMENT 'Average tip as % of fare',

    -- Trip Type Distribution
    long_trips INT COMMENT 'Count of trips with is_long_trip = 1',
    short_trips INT COMMENT 'Count of trips with is_short_trip = 1',
    high_tip_trips INT COMMENT 'Count of trips with high_tip_flag = 1',

    -- Rate Code Distribution
    standard_rate_trips INT COMMENT 'Trips with rate_code_id = 1',
    airport_trips INT COMMENT 'Trips with rate_code_id IN (2,3)',
    negotiated_trips INT COMMENT 'Trips with rate_code_id = 5',

    -- Payment Analysis
    credit_card_trips INT COMMENT 'Trips with payment_type = 1',
    cash_trips INT COMMENT 'Trips with payment_type = 2',

    -- Metadata & Quality
    data_quality_score DECIMAL(5,2) COMMENT 'Aggregated quality score 0–100',
    source_row_count INT COMMENT 'Count of source records aggregated',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Record creation timestamp',
    created_by VARCHAR(50) DEFAULT 'transform_v1.1.0' COMMENT 'ETL process identifier',

    -- Composite Unique Constraint
    UNIQUE KEY uk_summary (
        summary_date,
        vendor_id,
        pickup_zone_id,
        dropoff_zone_id
    ),

    -- Performance Indexes
    INDEX idx_date (summary_date),
    INDEX idx_vendor (vendor_id),
    INDEX idx_pickup_zone (pickup_zone_id),
    INDEX idx_pickup_borough (pickup_borough),
    INDEX idx_date_vendor (summary_date, vendor_id),
    INDEX idx_created (created_at)

) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Daily trip aggregations derived from pickup_dt by vendor and zones';
  
INSERT INTO analytics_daily_trip_summary (
    summary_date, vendor_id, vendor_name,
    pickup_zone_id, pickup_zone_name, pickup_borough,
    dropoff_zone_id, dropoff_zone_name, dropoff_borough,

    total_trips, total_passengers,

    total_revenue, total_fare, total_tips, total_tolls, total_surcharges,
    total_distance, avg_distance, avg_trip_duration,

    avg_fare, avg_tip, avg_total_amount,

    trips_with_tips, tip_percentage, avg_tip_pct,

    long_trips, short_trips, high_tip_trips,
    standard_rate_trips, airport_trips, negotiated_trips,

    data_quality_score, source_row_count
)
WITH validated_trips AS (
    SELECT 
        trip_id,

        /* Safe pickup datetime */
        CASE 
            WHEN pickup_dt IS NOT NULL
             AND pickup_dt REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$'
            THEN pickup_dt
            ELSE NULL
        END AS safe_pickup_dt,

        CASE 
            WHEN tpep_dropoff_datetime IS NOT NULL
             AND tpep_dropoff_datetime REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$'
            THEN tpep_dropoff_datetime
            ELSE NULL
        END AS safe_dropoff_dt,

        /* Vendor */
        CASE 
            WHEN vendorid REGEXP '^[0-9]+$'
            THEN CAST(vendorid AS UNSIGNED)
            ELSE NULL
        END AS safe_vendorid,

        /* Passengers */
        CASE 
            WHEN passenger_count REGEXP '^[0-9]+$'
            THEN CAST(passenger_count AS UNSIGNED)
            ELSE 0
        END AS safe_passenger_count,

        /* Distance & duration */
        CASE 
            WHEN trip_distance REGEXP '^-?[0-9]+\.?[0-9]*$'
            THEN CAST(trip_distance AS DECIMAL(10,2))
            ELSE 0
        END AS safe_trip_distance,

        CASE 
            WHEN trip_duration_minutes REGEXP '^-?[0-9]+\.?[0-9]*$'
            THEN CAST(trip_duration_minutes AS DECIMAL(10,2))
            ELSE 0
        END AS safe_trip_duration,

        /* Financials */
        CASE 
            WHEN fare_amount REGEXP '^-?[0-9]+\.?[0-9]*$'
            THEN CAST(fare_amount AS DECIMAL(10,2))
            ELSE 0
        END AS safe_fare_amount,

        CASE 
            WHEN tip_amount REGEXP '^-?[0-9]+\.?[0-9]*$'
            THEN CAST(tip_amount AS DECIMAL(10,2))
            ELSE 0
        END AS safe_tip_amount,

        CASE 
            WHEN total_amount REGEXP '^-?[0-9]+\.?[0-9]*$'
            THEN CAST(total_amount AS DECIMAL(10,2))
            ELSE 0
        END AS safe_total_amount,

        CASE 
            WHEN tolls_amount REGEXP '^-?[0-9]+\.?[0-9]*$'
            THEN CAST(tolls_amount AS DECIMAL(10,2))
            ELSE 0
        END AS safe_tolls_amount,

        /* Surcharges */
        (
            CASE 
                WHEN congestion_surcharge REGEXP '^-?[0-9]+\.?[0-9]*$'
                THEN CAST(congestion_surcharge AS DECIMAL(10,2))
                ELSE 0
            END +
            CASE 
                WHEN airport_fee REGEXP '^-?[0-9]+\.?[0-9]*$'
                THEN CAST(airport_fee AS DECIMAL(10,2))
                ELSE 0
            END
        ) AS safe_total_surcharges,

        /* Locations */
        CASE WHEN pulocationid REGEXP '^[0-9]+$' THEN CAST(pulocationid AS UNSIGNED) END AS safe_pulocationid,
        CASE WHEN dolocationid REGEXP '^[0-9]+$' THEN CAST(dolocationid AS UNSIGNED) END AS safe_dolocationid,

        IFNULL(pu_zone, 'Unknown') AS safe_pu_zone,
        IFNULL(pu_borough, 'Unknown') AS safe_pu_borough,
        IFNULL(do_zone, 'Unknown') AS safe_do_zone,
        IFNULL(do_borough, 'Unknown') AS safe_do_borough,

        /* Flags */
        CAST(IFNULL(is_long_trip, 0) AS UNSIGNED) AS safe_is_long_trip,
        CAST(IFNULL(is_short_trip, 0) AS UNSIGNED) AS safe_is_short_trip,
        CAST(IFNULL(high_tip_flag, 0) AS UNSIGNED) AS safe_high_tip_flag,

        CAST(ratecodeid AS UNSIGNED) AS safe_ratecodeid,
        CAST(payment_type AS UNSIGNED) AS safe_payment_type,
        CAST(IFNULL(tip_pct, 0) AS DECIMAL(10,2)) AS safe_tip_pct

    FROM mdm_golden_trip_level
    WHERE pickup_dt IS NOT NULL
      AND pickup_dt REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$'
	  AND YEAR(pickup_dt) = 2025
	  AND MONTH(pickup_dt) = 9

),

quality_scored_trips AS (
    SELECT 
        *,
        ROUND((
            (safe_total_amount >= 0) +
            (safe_fare_amount >= 0) +
            (safe_trip_distance >= 0) +
            (safe_pickup_dt < safe_dropoff_dt) +
            (safe_trip_duration BETWEEN 1 AND 1440) +
            (safe_passenger_count BETWEEN 1 AND 8) +
            (safe_pulocationid IS NOT NULL) +
            (safe_dolocationid IS NOT NULL) +
            (safe_vendorid IS NOT NULL) +
            (safe_pickup_dt IS NOT NULL)
        ) / 10 * 100, 2) AS row_quality_score,

        DATE(safe_pickup_dt) AS trip_date
    FROM validated_trips
),

enriched_trips AS (
    SELECT 
        t.*,
        IFNULL(v.vendor_name, 'Unknown Vendor') AS vendor_name
    FROM quality_scored_trips t
    LEFT JOIN mdm_vendor v
        ON t.safe_vendorid = v.source_vendor_id
       AND v.lifecycle_state = 'ACTIVE'
),

daily_aggregates AS (
    SELECT 
        trip_date,
        safe_vendorid AS vendorid,
        vendor_name,
        safe_pulocationid AS pickup_zone_id,
        safe_pu_zone AS pickup_zone_name,
        safe_pu_borough AS pickup_borough,
        safe_dolocationid AS dropoff_zone_id,
        safe_do_zone AS dropoff_zone_name,
        safe_do_borough AS dropoff_borough,

        COUNT(*) AS total_trips,
        -- Rate codes
		SUM(CASE WHEN safe_ratecodeid = 1 THEN 1 ELSE 0 END) AS standard_rate_trips,
		SUM(CASE WHEN safe_ratecodeid IN (2, 3) THEN 1 ELSE 0 END) AS airport_trips,
		SUM(CASE WHEN safe_ratecodeid = 5 THEN 1 ELSE 0 END) AS negotiated_trips,
        SUM(safe_passenger_count) AS total_passengers,
        SUM(safe_total_amount) AS total_revenue,
        SUM(safe_fare_amount) AS total_fare,
        SUM(safe_tip_amount) AS total_tips,
        SUM(safe_tolls_amount) AS total_tolls,
        SUM(safe_total_surcharges) AS total_surcharges,
        SUM(safe_trip_distance) AS total_distance,
        AVG(safe_trip_distance) AS avg_distance,
        AVG(safe_trip_duration) AS avg_trip_duration,
        AVG(safe_fare_amount) AS avg_fare,
        AVG(safe_tip_amount) AS avg_tip,
        AVG(safe_total_amount) AS avg_total_amount,
        SUM(safe_tip_amount > 0) AS trips_with_tips,
        AVG(safe_tip_pct) AS avg_tip_pct,
        SUM(safe_is_long_trip) AS long_trips,
        SUM(safe_is_short_trip) AS short_trips,
        SUM(safe_high_tip_flag) AS high_tip_trips,
        AVG(row_quality_score) AS data_quality_score,
        COUNT(*) AS source_row_count
    FROM enriched_trips
    GROUP BY
        trip_date, safe_vendorid, vendor_name,
        safe_pulocationid, safe_pu_zone, safe_pu_borough,
        safe_dolocationid, safe_do_zone, safe_do_borough
)

SELECT
    trip_date AS summary_date,
    vendorid  AS vendor_id,
    vendor_name,

    pickup_zone_id,
    pickup_zone_name,
    pickup_borough,

    dropoff_zone_id,
    dropoff_zone_name,
    dropoff_borough,

    total_trips,
    total_passengers,

    ROUND(total_revenue, 2),
    ROUND(total_fare, 2),
    ROUND(total_tips, 2),
    ROUND(total_tolls, 2),
    ROUND(total_surcharges, 2),

    ROUND(total_distance, 2),
    ROUND(avg_distance, 2),
    ROUND(avg_trip_duration, 2),

    ROUND(avg_fare, 2),
    ROUND(avg_tip, 2),
    ROUND(avg_total_amount, 2),

    trips_with_tips,
    ROUND(trips_with_tips * 100.0 / NULLIF(total_trips, 0), 2),
    ROUND(avg_tip_pct, 2),

    long_trips,
    short_trips,
    high_tip_trips,

    standard_rate_trips,
    airport_trips,
    negotiated_trips,

    ROUND(data_quality_score, 2),
    source_row_count
FROM daily_aggregates
WHERE trip_date IS NOT NULL
  AND vendorid IS NOT NULL
  AND pickup_zone_id IS NOT NULL;
select * from analytics_daily_trip_summary;
select count(*) from mdm_golden_trip_level;

SELECT
    YEAR(summary_date) AS year,
    MONTH(summary_date) AS month,
    COUNT(*) AS total_records
FROM analytics_daily_trip_summary
WHERE summary_date IS NOT NULL
  AND summary_date >= '2025-08-01'
  AND summary_date <= '2026-01-13'
GROUP BY YEAR(summary_date), MONTH(summary_date)
ORDER BY YEAR(summary_date), MONTH(summary_date);
