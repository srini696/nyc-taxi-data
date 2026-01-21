USE nyc_taxi_mdm;

DELIMITER //

/*
     STORED PROCEDURE 1: Calculate Completeness Score
     Calculate percentage of non-NULL values in a column
     CALL sp_calculate_completeness_score('mdm_vendor', 'vendor_name', @score);
     SELECT @score;
*/
DROP PROCEDURE IF EXISTS sp_calculate_completeness_score//

CREATE PROCEDURE sp_calculate_completeness_score(
    IN p_table_name VARCHAR(100),
    IN p_column_name VARCHAR(100),
    OUT p_score DECIMAL(5,2)
)
BEGIN
    DECLARE v_total INT;
    DECLARE v_non_null INT;

    SET @sql_total = CONCAT('SELECT COUNT(*) INTO @v_total FROM ', p_table_name);
    SET @sql_non_null = CONCAT('SELECT COUNT(*) INTO @v_non_null FROM ', p_table_name,
                               ' WHERE ', p_column_name, ' IS NOT NULL');

    PREPARE stmt_total FROM @sql_total;
    EXECUTE stmt_total;
    DEALLOCATE PREPARE stmt_total;

    PREPARE stmt_non_null FROM @sql_non_null;
    EXECUTE stmt_non_null;
    DEALLOCATE PREPARE stmt_non_null;

    SET v_total = @v_total;
    SET v_non_null = @v_non_null;

    IF v_total = 0 THEN
        SET p_score = 0;
    ELSE
        SET p_score = ROUND((v_non_null * 100.0) / v_total, 2);
    END IF;
END//

/*
   STORED PROCEDURE 2: Calculate Duplicate Rate
     Calculate percentage of duplicate records based on key column
     CALL sp_calculate_duplicate_rate('mdm_golden_trip_level', 'trip_id', @dup_rate);
     SELECT @dup_rate;
*/
DROP PROCEDURE IF EXISTS sp_calculate_duplicate_rate//

CREATE PROCEDURE sp_calculate_duplicate_rate(
    IN p_table_name VARCHAR(100),
    IN p_key_column VARCHAR(100),
    OUT p_duplicate_rate DECIMAL(5,2)
)
BEGIN
    DECLARE v_total INT;
    DECLARE v_unique INT;

    SET @sql_total = CONCAT('SELECT COUNT(*) INTO @v_total FROM ', p_table_name);
    SET @sql_unique = CONCAT('SELECT COUNT(DISTINCT ', p_key_column, ') INTO @v_unique FROM ', p_table_name);

    PREPARE stmt_total FROM @sql_total;
    EXECUTE stmt_total;
    DEALLOCATE PREPARE stmt_total;

    PREPARE stmt_unique FROM @sql_unique;
    EXECUTE stmt_unique;
    DEALLOCATE PREPARE stmt_unique;

    SET v_total = @v_total;
    SET v_unique = @v_unique;

    IF v_total = 0 THEN
        SET p_duplicate_rate = 0;
    ELSE
        SET p_duplicate_rate = ROUND(((v_total - v_unique) * 100.0) / v_total, 2);
    END IF;
END//

/* 
   STORED PROCEDURE 3: Check Data Freshness
   Determine if data has been updated within acceptable window
  
   CALL sp_check_data_freshness('mdm_vendor', 'updated_at', 7, @status);
     SELECT @status;
*/
DROP PROCEDURE IF EXISTS sp_check_data_freshness//

CREATE PROCEDURE sp_check_data_freshness(
    IN p_table_name VARCHAR(100),
    IN p_timestamp_column VARCHAR(100),
    IN p_max_days_old INT,
    OUT p_status VARCHAR(10)
)
BEGIN
    DECLARE v_latest_update DATETIME;
    DECLARE v_days_old INT;

    SET @sql = CONCAT('SELECT MAX(', p_timestamp_column, ') INTO @v_latest FROM ', p_table_name);

    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET v_latest_update = @v_latest;

    IF v_latest_update IS NULL THEN
        SET p_status = 'STALE';
    ELSE
        SET v_days_old = DATEDIFF(NOW(), v_latest_update);
        IF v_days_old <= p_max_days_old THEN
            SET p_status = 'FRESH';
        ELSE
            SET p_status = 'STALE';
        END IF;
    END IF;
END//

DELIMITER ;

-- Completeness
CALL sp_calculate_completeness_score('mdm_vendor', 'vendor_name', @score);
SELECT @score AS completeness_pct;

-- Duplicate rate
CALL sp_calculate_duplicate_rate('mdm_vendor', 'vendor_name', @dup_rate);
SELECT @dup_rate AS duplicate_rate_pct;

-- Data freshness
CALL sp_check_data_freshness('mdm_vendor', 'updated_at', 7, @status);
SELECT @status AS freshness_status;
