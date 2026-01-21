

USE nyc_taxi_mdm;


-- Vendor Performance Summary

/*
Business Question: What is each vendor's performance across key metrics?
Refresh: Daily full, 15-min incremental
Expiry: Data older than 1 year
*/

DROP TABLE IF EXISTS mv_vendor_performance_summary;

CREATE TABLE mv_vendor_performance_summary (
    vendor_id INT NOT NULL,
    vendor_name VARCHAR(200),
    metric_date DATE NOT NULL,
    total_trips INT DEFAULT 0,
    total_revenue DECIMAL(15,2) DEFAULT 0.00,
    total_distance DECIMAL(12,2) DEFAULT 0.00,
    avg_trip_duration DECIMAL(10,2),
    avg_fare DECIMAL(10,2),
    avg_tip_pct DECIMAL(5,2),
    customer_satisfaction_score DECIMAL(5,2),
    on_time_pct DECIMAL(5,2),
    
    -- Metadata
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    refresh_type VARCHAR(20), -- 'FULL' or 'INCREMENTAL'
    source_row_count INT,
    
    PRIMARY KEY (vendor_id, metric_date),
    INDEX idx_metric_date (metric_date),
    INDEX idx_last_updated (last_updated)
) ENGINE=InnoDB;


-- REFRESH PROCEDURE: Full Refresh


DELIMITER //

DROP PROCEDURE IF EXISTS sp_refresh_vendor_performance_full//

CREATE PROCEDURE sp_refresh_vendor_performance_full()
BEGIN
    DECLARE v_start_time TIMESTAMP;
    DECLARE v_rows_processed INT;
    DECLARE v_execution_status VARCHAR(20);
    
    SET v_start_time = NOW();
    
    START TRANSACTION;
    
    -- Clear existing data older than 1 year
    DELETE FROM mv_vendor_performance_summary 
    WHERE metric_date < DATE_SUB(CURDATE(), INTERVAL 1 YEAR);
    
    -- Rebuild from source
    INSERT INTO mv_vendor_performance_summary (
        vendor_id, vendor_name, metric_date, total_trips, total_revenue,
        total_distance, avg_trip_duration, avg_fare, avg_tip_pct,
        refresh_type, source_row_count
    )
    SELECT 
        t.vendorid,
        v.vendor_name,
        DATE(t.tpep_pickup_datetime) AS metric_date,
        COUNT(*) AS total_trips,
        ROUND(SUM(t.total_amount), 2) AS total_revenue,
        ROUND(SUM(t.trip_distance), 2) AS total_distance,
        ROUND(AVG(TIMESTAMPDIFF(MINUTE, t.tpep_pickup_datetime, t.tpep_dropoff_datetime)), 2) AS avg_trip_duration,
        ROUND(AVG(t.fare_amount), 2) AS avg_fare,
        ROUND(AVG(CASE 
            WHEN t.fare_amount > 0 THEN (t.tip_amount / t.fare_amount) * 100 
            ELSE 0 
        END), 2) AS avg_tip_pct,
        'FULL' AS refresh_type,
        COUNT(*) AS source_row_count
    FROM mdm_golden_trip_level t
    LEFT JOIN mdm_vendor v ON t.vendorid = v.source_vendor_id AND v.lifecycle_state = 'ACTIVE'
    WHERE t.tpep_pickup_datetime >= DATE_SUB(CURDATE(), INTERVAL 1 YEAR)
      AND t.tpep_pickup_datetime IS NOT NULL
      AND t.vendorid IS NOT NULL
    GROUP BY 
        t.vendorid,
        v.vendor_name,
        DATE(t.tpep_pickup_datetime)
    ON DUPLICATE KEY UPDATE
        vendor_name = VALUES(vendor_name),
        total_trips = VALUES(total_trips),
        total_revenue = VALUES(total_revenue),
        total_distance = VALUES(total_distance),
        avg_trip_duration = VALUES(avg_trip_duration),
        avg_fare = VALUES(avg_fare),
        avg_tip_pct = VALUES(avg_tip_pct),
        refresh_type = 'FULL',
        source_row_count = VALUES(source_row_count);
    
    SET v_rows_processed = ROW_COUNT();
    SET v_execution_status = 'SUCCESS';
    
    COMMIT;
    
    -- Log execution
    INSERT INTO transformation_execution_log (
        transformation_name, version, execution_start, execution_end,
        status, rows_processed, executed_by
    ) VALUES (
        'mv_vendor_performance_summary_full', '1.0.0',
        v_start_time, NOW(), v_execution_status, v_rows_processed,
        'materialized_view_refresh'
    );
    
    SELECT 
        'Full Refresh Complete' AS message,
        v_rows_processed AS rows_processed,
        TIMESTAMPDIFF(SECOND, v_start_time, NOW()) AS duration_seconds;
        
END//


-- REFRESH PROCEDURE: Incremental Refresh (Last 24 Hours)


DROP PROCEDURE IF EXISTS sp_refresh_vendor_performance_incremental//

CREATE PROCEDURE sp_refresh_vendor_performance_incremental()
BEGIN
    DECLARE v_start_time TIMESTAMP;
    DECLARE v_rows_processed INT;
    
    SET v_start_time = NOW();
    
    START TRANSACTION;
    
    -- Update only records from last 24 hours
    INSERT INTO mv_vendor_performance_summary (
        vendor_id, vendor_name, metric_date, total_trips, total_revenue,
        total_distance, avg_trip_duration, avg_fare, avg_tip_pct,
        refresh_type, source_row_count
    )
    SELECT 
        t.vendorid,
        v.vendor_name,
        DATE(t.tpep_pickup_datetime) AS metric_date,
        COUNT(*) AS total_trips,
        ROUND(SUM(t.total_amount), 2) AS total_revenue,
        ROUND(SUM(t.trip_distance), 2) AS total_distance,
        ROUND(AVG(TIMESTAMPDIFF(MINUTE, t.tpep_pickup_datetime, t.tpep_dropoff_datetime)), 2) AS avg_trip_duration,
        ROUND(AVG(t.fare_amount), 2) AS avg_fare,
        ROUND(AVG(CASE 
            WHEN t.fare_amount > 0 THEN (t.tip_amount / t.fare_amount) * 100 
            ELSE 0 
        END), 2) AS avg_tip_pct,
        'INCREMENTAL' AS refresh_type,
        COUNT(*) AS source_row_count
    FROM mdm_golden_trip_level t
    LEFT JOIN mdm_vendor v ON t.vendorid = v.source_vendor_id AND v.lifecycle_state = 'ACTIVE'
    WHERE t.tpep_pickup_datetime >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
      AND t.tpep_pickup_datetime IS NOT NULL
      AND t.vendorid IS NOT NULL
    GROUP BY 
        t.vendorid,
        v.vendor_name,
        DATE(t.tpep_pickup_datetime)
    ON DUPLICATE KEY UPDATE
        vendor_name = VALUES(vendor_name),
        total_trips = VALUES(total_trips),
        total_revenue = VALUES(total_revenue),
        total_distance = VALUES(total_distance),
        avg_trip_duration = VALUES(avg_trip_duration),
        avg_fare = VALUES(avg_fare),
        avg_tip_pct = VALUES(avg_tip_pct),
        refresh_type = 'INCREMENTAL',
        source_row_count = VALUES(source_row_count);
    
    SET v_rows_processed = ROW_COUNT();
    
    COMMIT;
    
    SELECT 
        'Incremental Refresh Complete' AS message,
        v_rows_processed AS rows_affected,
        TIMESTAMPDIFF(SECOND, v_start_time, NOW()) AS duration_seconds;
        
END//


-- MATERIALIZED VIEW 2: Zone-to-Zone Trip Patterns


DROP TABLE IF EXISTS mv_zone_trip_patterns//

CREATE TABLE mv_zone_trip_patterns (
    pickup_zone_id INT NOT NULL,
    dropoff_zone_id INT NOT NULL,
    metric_date DATE NOT NULL,
    trip_count INT DEFAULT 0,
    avg_fare DECIMAL(10,2),
    avg_distance DECIMAL(10,2),
    avg_duration DECIMAL(10,2),
    peak_hour INT, -- Hour with most trips
    
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    refresh_type VARCHAR(20),
    
    PRIMARY KEY (pickup_zone_id, dropoff_zone_id, metric_date),
    INDEX idx_metric_date (metric_date),
    INDEX idx_pickup (pickup_zone_id),
    INDEX idx_dropoff (dropoff_zone_id)
) ENGINE=InnoDB;

DROP PROCEDURE IF EXISTS sp_refresh_zone_patterns_full//

CREATE PROCEDURE sp_refresh_zone_patterns_full()
BEGIN
    START TRANSACTION;
    
    DELETE FROM mv_zone_trip_patterns 
    WHERE metric_date < DATE_SUB(CURDATE(), INTERVAL 90 DAY);
    
    INSERT INTO mv_zone_trip_patterns (
        pickup_zone_id, dropoff_zone_id, metric_date, trip_count,
        avg_fare, avg_distance, avg_duration, peak_hour, refresh_type
    )
    SELECT 
        t.pulocationid AS pickup_zone_id,
        t.dolocationid AS dropoff_zone_id,
        DATE(t.tpep_pickup_datetime) AS metric_date,
        COUNT(*) AS trip_count,
        ROUND(AVG(t.fare_amount), 2) AS avg_fare,
        ROUND(AVG(t.trip_distance), 2) AS avg_distance,
        ROUND(AVG(TIMESTAMPDIFF(MINUTE, t.tpep_pickup_datetime, t.tpep_dropoff_datetime)), 2) AS avg_duration,
        (SELECT HOUR(tpep_pickup_datetime) 
         FROM mdm_golden_trip_level 
         WHERE pulocationid = t.pulocationid 
           AND dolocationid = t.dolocationid
           AND DATE(tpep_pickup_datetime) = DATE(t.tpep_pickup_datetime)
         GROUP BY HOUR(tpep_pickup_datetime)
         ORDER BY COUNT(*) DESC
         LIMIT 1) AS peak_hour,
        'FULL' AS refresh_type
    FROM mdm_golden_trip_level t
    WHERE t.tpep_pickup_datetime >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
      AND t.pulocationid IS NOT NULL
      AND t.dolocationid IS NOT NULL
    GROUP BY 
        t.pulocationid,
        t.dolocationid,
        DATE(t.tpep_pickup_datetime)
    ON DUPLICATE KEY UPDATE
        trip_count = VALUES(trip_count),
        avg_fare = VALUES(avg_fare),
        avg_distance = VALUES(avg_distance),
        avg_duration = VALUES(avg_duration),
        peak_hour = VALUES(peak_hour),
        refresh_type = 'FULL';
    
    COMMIT;
    
    SELECT 'Zone Pattern Refresh Complete' AS message;
END//

DELIMITER ;


-- AUTOMATED SCHEDULING WITH EVENTS


-- Enable event scheduler (run once)
-- SET GLOBAL event_scheduler = ON;

-- Schedule 1: Full refresh daily at 2 AM
DROP EVENT IF EXISTS evt_vendor_perf_full_refresh;

CREATE EVENT evt_vendor_perf_full_refresh
ON SCHEDULE EVERY 1 DAY
STARTS TIMESTAMP(CURRENT_DATE + INTERVAL 1 DAY) + INTERVAL 2 HOUR
DO CALL sp_refresh_vendor_performance_full();

-- Schedule 2: Incremental refresh every 15 minutes (8 AM - 8 PM)
DROP EVENT IF EXISTS evt_vendor_perf_incremental_refresh;

-- Change delimiter so MySQL knows where the event body ends
DELIMITER //

CREATE EVENT evt_vendor_perf_incremental_refresh
ON SCHEDULE EVERY 15 MINUTE
STARTS CURRENT_TIMESTAMP
DO
BEGIN
    -- Only run between 8 AM and 8 PM
    IF HOUR(NOW()) BETWEEN 8 AND 20 THEN
        CALL sp_refresh_vendor_performance_incremental();
    END IF;
END//

DELIMITER ;


-- Schedule 3: Zone patterns full refresh daily at 3 AM
DROP EVENT IF EXISTS evt_zone_patterns_refresh;

CREATE EVENT evt_zone_patterns_refresh
ON SCHEDULE EVERY 1 DAY
STARTS TIMESTAMP(CURRENT_DATE + INTERVAL 1 DAY) + INTERVAL 3 HOUR
DO CALL sp_refresh_zone_patterns_full();


-- USAGE EXAMPLES & QUERIES


-- Manual refresh commands
CALL sp_refresh_vendor_performance_full();
CALL sp_refresh_vendor_performance_incremental();
CALL sp_refresh_zone_patterns_full();

-- Query materialized view (fast!)
SELECT 
    vendor_name,
    metric_date,
    total_trips,
    total_revenue,
    avg_fare,
    avg_tip_pct
FROM mv_vendor_performance_summary
WHERE metric_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
  AND vendor_name IS NOT NULL
ORDER BY metric_date DESC, total_revenue DESC
LIMIT 20;

-- Top 10 busiest routes
SELECT 
    pz.zone_name AS pickup_zone,
    dz.zone_name AS dropoff_zone,
    SUM(zp.trip_count) AS total_trips,
    AVG(zp.avg_fare) AS avg_fare,
    AVG(zp.avg_distance) AS avg_distance_miles
FROM mv_zone_trip_patterns zp
LEFT JOIN mdm_zone pz ON zp.pickup_zone_id = pz.source_zone_id
LEFT JOIN mdm_zone dz ON zp.dropoff_zone_id = dz.source_zone_id
WHERE zp.metric_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
GROUP BY pickup_zone, dropoff_zone
ORDER BY total_trips DESC
LIMIT 10;

-- Check materialized view freshness
SELECT 
    'mv_vendor_performance_summary' AS view_name,
    MAX(last_updated) AS last_refresh,
    TIMESTAMPDIFF(MINUTE, MAX(last_updated), NOW()) AS minutes_since_refresh,
    COUNT(*) AS total_records
FROM mv_vendor_performance_summary
UNION ALL
SELECT 
    'mv_zone_trip_patterns',
    MAX(last_updated),
    TIMESTAMPDIFF(MINUTE, MAX(last_updated), NOW()),
    COUNT(*)
FROM mv_zone_trip_patterns;

-- View scheduled events
SHOW EVENTS WHERE Db = 'nyc_taxi_mdm';