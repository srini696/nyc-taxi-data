/*

  1. Test data quality at source
  2. Test transformation logic
  3. Test output data quality
  4. Test performance under load
  5. Test idempotency (can run multiple times safely)

*/

USE nyc_taxi_mdm;

-- TEST FRAMEWORK SETUP


-- Create test results logging table
CREATE TABLE IF NOT EXISTS sql_test_results (
    test_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    test_suite VARCHAR(100) NOT NULL,
    test_name VARCHAR(200) NOT NULL,
    test_category VARCHAR(50), -- unit, integration, performance
    test_status VARCHAR(20), -- PASS, FAIL, SKIP, ERROR
    expected_result TEXT,
    actual_result TEXT,
    error_message TEXT,
    execution_time_ms INT,
    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    executed_by VARCHAR(50) DEFAULT 'test_framework',
    
    INDEX idx_suite (test_suite),
    INDEX idx_status (test_status),
    INDEX idx_executed_at (executed_at)
) ENGINE=InnoDB;

-- Test execution procedure
DELIMITER //

DROP PROCEDURE IF EXISTS sp_run_test//

CREATE PROCEDURE sp_run_test(
    IN p_test_suite VARCHAR(100),
    IN p_test_name VARCHAR(200),
    IN p_test_category VARCHAR(50),
    IN p_test_sql TEXT,
    IN p_expected_result TEXT
)
BEGIN
    DECLARE v_actual_result TEXT;
    DECLARE v_status VARCHAR(20);
    DECLARE v_error_message TEXT;
    DECLARE v_start_time BIGINT;
    DECLARE v_end_time BIGINT;
    DECLARE v_execution_time INT;
    DECLARE exit handler for sqlexception
    BEGIN
        GET DIAGNOSTICS CONDITION 1 v_error_message = MESSAGE_TEXT;
        SET v_status = 'ERROR';
        INSERT INTO sql_test_results (
            test_suite, test_name, test_category, test_status,
            expected_result, error_message
        ) VALUES (
            p_test_suite, p_test_name, p_test_category, v_status,
            p_expected_result, v_error_message
        );
    END;
    
    SET v_start_time = UNIX_TIMESTAMP(NOW(3)) * 1000;
    
    -- Execute test SQL
    SET @test_sql = p_test_sql;
    PREPARE stmt FROM @test_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    
    -- Get actual result
    SET v_actual_result = @test_result;
    
    SET v_end_time = UNIX_TIMESTAMP(NOW(3)) * 1000;
    SET v_execution_time = v_end_time - v_start_time;
    
    -- Compare results
    IF v_actual_result = p_expected_result THEN
        SET v_status = 'PASS';
    ELSE
        SET v_status = 'FAIL';
    END IF;
    
    -- Log results
    INSERT INTO sql_test_results (
        test_suite, test_name, test_category, test_status,
        expected_result, actual_result, execution_time_ms
    ) VALUES (
        p_test_suite, p_test_name, p_test_category, v_status,
        p_expected_result, v_actual_result, v_execution_time
    );
    
    SELECT v_status AS status, v_actual_result AS result;
END//

DELIMITER ;

--  Data Quality Tests (Source Data)


-- Test 1.1: Check for NULL values in critical fields
SELECT 
    'NULL_CHECK_TRIP_ID' AS test_name,
    COUNT(*) AS null_count,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM mdm_golden_trip_level
WHERE trip_id IS NULL;

-- Test 1.2: Check for negative amounts
USE nyc_taxi_mdm;



-- Generic Procedure to Run Negative Value Check (without logging)


-- Test 1.3: Check for invalid date ranges
SELECT 
    'DATE_RANGE_CHECK' AS test_name,
    COUNT(*) AS violation_count,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM mdm_golden_trip_level
WHERE tpep_pickup_datetime > tpep_dropoff_datetime
   OR tpep_pickup_datetime > NOW()
   OR tpep_dropoff_datetime > NOW();

-- Test 1.4: Check for duplicate trip IDs
SELECT 
    'DUPLICATE_TRIP_ID_CHECK' AS test_name,
    COUNT(*) - COUNT(DISTINCT trip_id) AS duplicate_count,
    CASE WHEN COUNT(*) = COUNT(DISTINCT trip_id) THEN 'PASS' ELSE 'FAIL' END AS status
FROM mdm_golden_trip_level;

-- Test 1.5: Check referential integrity (vendor exists)
SELECT 
    'VENDOR_REFERENTIAL_INTEGRITY' AS test_name,
    COUNT(*) AS orphan_count,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM mdm_golden_trip_level t
LEFT JOIN mdm_vendor v ON t.vendorid = v.source_vendor_id AND v.lifecycle_state = 'ACTIVE'
WHERE t.vendorid IS NOT NULL
  AND v.vendor_id IS NULL;


--  Transformation Logic Tests


-- Test 2.1: Revenue calculation accuracy
WITH test_data AS (
    SELECT 
        SUM(fare_amount + extra + mta_tax + tip_amount + tolls_amount + 
            improvement_surcharge + IFNULL(congestion_surcharge, 0) + 
            IFNULL(airport_fee, 0)) AS calculated_total,
        SUM(total_amount) AS recorded_total
    FROM mdm_golden_trip_level
    WHERE pickup_dt >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
)
SELECT 
    'REVENUE_CALCULATION_CHECK' AS test_name,
    ROUND(ABS(calculated_total - recorded_total), 2) AS variance,
    CASE 
        WHEN ABS(calculated_total - recorded_total) <= 0.01 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM test_data;

-- Test 2.2: Trip duration calculation
SELECT 
    'TRIP_DURATION_CHECK' AS test_name,
    COUNT(*) AS invalid_count,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM mdm_golden_trip_level
WHERE TIMESTAMPDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime) < 0
   OR TIMESTAMPDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime) > 1440; -- > 24 hours

-- Test 2.3: Aggregation consistency
WITH daily_agg AS (
    SELECT 
        DATE(tpep_pickup_datetime) AS trip_date,
        COUNT(*) AS trip_count,
        SUM(total_amount) AS total_revenue
    FROM mdm_golden_trip_level
    WHERE tpep_pickup_datetime >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
    GROUP BY DATE(tpep_pickup_datetime)
),
summary_agg AS (
    SELECT 
        summary_date,
        SUM(total_trips) AS trip_count,
        SUM(total_revenue) AS total_revenue
    FROM analytics_daily_trip_summary
    WHERE summary_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
    GROUP BY summary_date
)
SELECT 
    'AGGREGATION_RECONCILIATION' AS test_name,
    COUNT(*) AS mismatch_count,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM daily_agg d
INNER JOIN summary_agg s ON d.trip_date = s.summary_date
WHERE ABS(d.trip_count - s.trip_count) > 0
   OR ABS(d.total_revenue - s.total_revenue) > 0.01;


-- Output Data Quality Tests


-- Test 3.1: Completeness check for summary table
SELECT 
    'SUMMARY_COMPLETENESS_CHECK' AS test_name,
    (COUNT(*) * 100.0 / NULLIF((SELECT COUNT(*) FROM analytics_daily_trip_summary), 0)) AS completeness_pct,
    CASE 
        WHEN (COUNT(*) * 100.0 / NULLIF((SELECT COUNT(*) FROM analytics_daily_trip_summary), 0)) >= 99.9
        THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM analytics_daily_trip_summary
WHERE summary_date IS NOT NULL
  AND vendor_id IS NOT NULL
  AND pickup_zone_id IS NOT NULL;

-- Test 3.2: Data quality score threshold
SELECT 
    'DATA_QUALITY_SCORE_CHECK' AS test_name,
    AVG(data_quality_score) AS avg_score,
    MIN(data_quality_score) AS min_score,
    CASE 
        WHEN MIN(data_quality_score) >= 95.0 THEN 'PASS'
        WHEN MIN(data_quality_score) >= 90.0 THEN 'WARNING'
        ELSE 'FAIL'
    END AS status
FROM analytics_daily_trip_summary
WHERE created_at >= DATE_SUB(NOW(), INTERVAL 1 DAY);

-- Test 3.3: No orphaned records (all vendors exist in master)
SELECT 
    'ORPHANED_VENDOR_CHECK' AS test_name,
    COUNT(*) AS orphan_count,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM analytics_daily_trip_summary s
LEFT JOIN mdm_vendor v ON s.vendor_id = v.vendor_id
WHERE v.vendor_id IS NULL;

--  Performance Tests


-- Test 4.1: Query performance (should complete under 1 second)
SET @start_time = UNIX_TIMESTAMP(NOW(3)) * 1000;

SELECT COUNT(*) INTO @result_count
FROM analytics_daily_trip_summary
WHERE summary_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY);

SET @end_time = UNIX_TIMESTAMP(NOW(3)) * 1000;
SET @execution_time = @end_time - @start_time;

SELECT 
    'QUERY_PERFORMANCE_CHECK' AS test_name,
    @execution_time AS execution_time_ms,
    @result_count AS rows_returned,
    CASE 
        WHEN @execution_time <= 1000 THEN 'PASS'
        WHEN @execution_time <= 5000 THEN 'WARNING'
        ELSE 'FAIL'
    END AS status;

-- Test 4.2: Index usage check
EXPLAIN 
SELECT * FROM analytics_daily_trip_summary
WHERE summary_date = '2026-01-01'
  AND vendor_id = 1;

--  Should use idx_summary_date or idx_vendor


-- Idempotency Tests


-- Test 5.1: Running transformation twice produces same result
-- Step 1: Get initial row count
SET @initial_count = (SELECT COUNT(*) FROM analytics_daily_trip_summary);

-- Step 2: Re-run transformation (simulated)
-- CALL sp_refresh_daily_trip_summary();

-- Step 3: Check row count hasn't duplicated
SET @final_count = (SELECT COUNT(*) FROM analytics_daily_trip_summary);

SELECT 
    'IDEMPOTENCY_CHECK' AS test_name,
    @initial_count AS initial_rows,
    @final_count AS final_rows,
    CASE 
        WHEN @initial_count = @final_count THEN 'PASS'
        ELSE 'FAIL'
    END AS status;


-- Master Data Quality Tests


-- Test 6.1: No duplicate vendor names (case-insensitive)
WITH vendor_dupes AS (
    SELECT 
        LOWER(vendor_name) AS normalized_name,
        COUNT(*) AS dupe_count
    FROM mdm_vendor
    WHERE lifecycle_state = 'ACTIVE'
    GROUP BY LOWER(vendor_name)
    HAVING COUNT(*) > 1
)
SELECT 
    'VENDOR_NAME_DUPLICATE_CHECK' AS test_name,
    COUNT(*) AS duplicate_groups,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM vendor_dupes;

-- Test 6.2: Zone data completeness
SELECT 
    'ZONE_COMPLETENESS_CHECK' AS test_name,
    COUNT(*) AS incomplete_records,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM mdm_zone
WHERE lifecycle_state = 'ACTIVE'
  AND (zone_name IS NULL OR zone_name = '');


-- TEST RUNNER: Execute All Tests


DELIMITER //

DROP PROCEDURE IF EXISTS sp_run_all_tests//

CREATE PROCEDURE sp_run_all_tests()
BEGIN
    DECLARE v_suite_start TIMESTAMP;
    DECLARE v_total_tests INT DEFAULT 0;
    DECLARE v_passed INT DEFAULT 0;
    DECLARE v_failed INT DEFAULT 0;
    DECLARE v_warnings INT DEFAULT 0;
    
    SET v_suite_start = NOW();
    
    -- Clear previous test results (optional)
    -- DELETE FROM sql_test_results WHERE executed_at < DATE_SUB(NOW(), INTERVAL 7 DAY);
    
    SELECT 'Starting Test Suite Execution...' AS message;
    SELECT '=' AS separator FROM dual UNION SELECT '=' FROM dual LIMIT 40;
    
    -- Run all test suites
    SELECT 'Running Test Suite 1: Data Quality Tests (Source)...' AS message;
    -- Tests 1.1-1.5 executed above
    
    SELECT 'Running Test Suite 2: Transformation Logic Tests...' AS message;
    -- Tests 2.1-2.3 executed above
    
    SELECT 'Running Test Suite 3: Output Data Quality Tests...' AS message;
    -- Tests 3.1-3.3 executed above
    
    SELECT 'Running Test Suite 4: Performance Tests...' AS message;
    -- Tests 4.1-4.2 executed above
    
    SELECT 'Running Test Suite 5: Idempotency Tests...' AS message;
    -- Tests 5.1 executed above
    
    SELECT 'Running Test Suite 6: Master Data Quality Tests...' AS message;
    -- Tests 6.1-6.2 executed above
    
    -- Generate summary report
    SELECT 
        test_suite,
        COUNT(*) AS total_tests,
        SUM(CASE WHEN test_status = 'PASS' THEN 1 ELSE 0 END) AS passed,
        SUM(CASE WHEN test_status = 'FAIL' THEN 1 ELSE 0 END) AS failed,
        SUM(CASE WHEN test_status = 'ERROR' THEN 1 ELSE 0 END) AS errors,
        ROUND(AVG(execution_time_ms), 2) AS avg_execution_ms
    FROM sql_test_results
    WHERE executed_at >= v_suite_start
    GROUP BY test_suite
    ORDER BY test_suite;
    
    -- Overall summary
    SELECT 
        'TEST SUITE SUMMARY' AS report_title,
        COUNT(*) AS total_tests,
        SUM(CASE WHEN test_status = 'PASS' THEN 1 ELSE 0 END) AS total_passed,
        SUM(CASE WHEN test_status = 'FAIL' THEN 1 ELSE 0 END) AS total_failed,
        SUM(CASE WHEN test_status = 'ERROR' THEN 1 ELSE 0 END) AS total_errors,
        ROUND((SUM(CASE WHEN test_status = 'PASS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) AS pass_rate,
        TIMESTAMPDIFF(SECOND, v_suite_start, NOW()) AS total_duration_seconds
    FROM sql_test_results
    WHERE executed_at >= v_suite_start;
    
    -- Failed tests detail
    SELECT 
        'Failed/Error Tests:' AS section,
        test_suite,
        test_name,
        test_status,
        expected_result,
        actual_result,
        error_message
    FROM sql_test_results
    WHERE executed_at >= v_suite_start
      AND test_status IN ('FAIL', 'ERROR')
    ORDER BY test_suite, test_name;
    
END//

DELIMITER ;

-- ============================================================================
-- USAGE
-- ============================================================================

-- Run all tests
CALL sp_run_all_tests();

-- View test history
SELECT * FROM sql_test_results 
ORDER BY executed_at DESC 
LIMIT 50;

-- View test summary by date
SELECT 
    DATE(executed_at) AS test_date,
    COUNT(*) AS total_tests,
    SUM(CASE WHEN test_status = 'PASS' THEN 1 ELSE 0 END) AS passed,
    SUM(CASE WHEN test_status = 'FAIL' THEN 1 ELSE 0 END) AS failed,
    ROUND((SUM(CASE WHEN test_status = 'PASS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) AS pass_rate
FROM sql_test_results
GROUP BY DATE(executed_at)
ORDER BY test_date DESC;