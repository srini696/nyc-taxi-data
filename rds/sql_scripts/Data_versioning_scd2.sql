-- ============================================================================
-- Slowly Changing Dimensions (SCD) - Complete Implementation Guide
-- Version: 1.0.0
-- Author: MDM Pipeline Team
-- Description: Implementation of SCD Types 0-6 for Master Data Management
-- ============================================================================

-- ============================================================================
-- SCD TYPE OVERVIEW
-- ============================================================================
/*
SCD Type 0: Fixed/Retain Original
- Data never changes after initial load
- Use case: Birth date, SSN, original creation date

SCD Type 1: Overwrite
- Replace old value with new value
- No history maintained
- Use case: Correcting errors, non-critical attributes

SCD Type 2: Add New Row (Most Common for MDM)
- Create new record for each change
- Maintains complete history
- Use case: Vendor name changes, zone boundary changes

SCD Type 3: Add New Column
- Add column for previous value
- Limited history (usually just previous)
- Use case: Previous address, previous manager

SCD Type 4: History Table
- Current values in main table
- History in separate table
- Use case: Separating operational and historical data

SCD Type 5: Mini-Dimension + Type 1
- Combines Type 4 with Type 1 outrigger
- Use case: Rapidly changing attributes

SCD Type 6: Hybrid (Type 1 + 2 + 3)
- Current flag + effective dates + previous value column
- Most comprehensive tracking
- Use case: Full audit requirements
*/

-- ============================================================================
-- SCD TYPE 2 IMPLEMENTATION FOR MDM_VENDOR
-- ============================================================================

-- Drop existing table and recreate with SCD Type 2 structure
DROP TABLE IF EXISTS mdm_vendor_scd2;

CREATE TABLE mdm_vendor_scd2 (
    -- Surrogate key (auto-generated)
    vendor_sk BIGINT AUTO_INCREMENT PRIMARY KEY,
    
    -- Natural/Business key
    source_vendor_id INT NOT NULL,
    
    -- Dimensional attributes
    vendor_name VARCHAR(200) NOT NULL,
    lifecycle_state VARCHAR(20) NOT NULL DEFAULT 'PROPOSED',
    match_confidence DECIMAL(5,2),
    source_system VARCHAR(50),
    
    -- SCD Type 2 control columns
    effective_start_date DATETIME NOT NULL,
    effective_end_date DATETIME DEFAULT '9999-12-31 23:59:59',
    is_current BOOLEAN DEFAULT TRUE,
    version_number INT DEFAULT 1,
    
    -- Audit columns
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(50) NOT NULL,
    updated_at TIMESTAMP NULL,
    updated_by VARCHAR(50),
    change_reason TEXT,
    
    -- SCD Type 3 addition (previous value)
    previous_vendor_name VARCHAR(200),
    
    -- Indexes for performance
    INDEX idx_source_vendor (source_vendor_id),
    INDEX idx_current (is_current),
    INDEX idx_effective_dates (effective_start_date, effective_end_date),
    INDEX idx_lifecycle (lifecycle_state),
    
    -- Unique constraint: only one current record per source_vendor_id
    UNIQUE KEY uk_vendor_current (source_vendor_id, is_current, effective_end_date)
);

-- ============================================================================
-- STORED PROCEDURE: SCD Type 2 Upsert for Vendors
-- ============================================================================
DROP PROCEDURE sp_upsert_vendor_scd2;
DELIMITER //

CREATE PROCEDURE sp_upsert_vendor_scd2(
    IN p_source_vendor_id INT,
    IN p_vendor_name VARCHAR(200),
    IN p_lifecycle_state VARCHAR(20),
    IN p_match_confidence DECIMAL(5,2),
    IN p_source_system VARCHAR(50),
    IN p_changed_by VARCHAR(50),
    IN p_change_reason TEXT
)
BEGIN
    DECLARE v_existing_sk BIGINT;
    DECLARE v_existing_name VARCHAR(200);
    DECLARE v_existing_state VARCHAR(20);
    DECLARE v_current_version INT;
    DECLARE v_has_changes BOOLEAN DEFAULT FALSE;
    
    -- Check if current record exists
    SELECT vendor_sk, vendor_name, lifecycle_state, version_number
    INTO v_existing_sk, v_existing_name, v_existing_state, v_current_version
    FROM mdm_vendor_scd2
    WHERE source_vendor_id = p_source_vendor_id
      AND is_current = TRUE
    LIMIT 1;
    
    -- Check if there are actual changes
    IF v_existing_sk IS NOT NULL THEN
        IF v_existing_name != p_vendor_name OR v_existing_state != p_lifecycle_state THEN
            SET v_has_changes = TRUE;
        END IF;
    END IF;
    
    IF v_existing_sk IS NULL THEN
        -- INSERT: New record (first version)
        INSERT INTO mdm_vendor_scd2 (
            source_vendor_id, vendor_name, lifecycle_state, match_confidence,
            source_system, effective_start_date, effective_end_date, is_current,
            version_number, created_by, change_reason
        ) VALUES (
            p_source_vendor_id, p_vendor_name, p_lifecycle_state, p_match_confidence,
            p_source_system, NOW(), '9999-12-31 23:59:59', TRUE,
            1, p_changed_by, p_change_reason
        );
        
    ELSEIF v_has_changes THEN
        -- UPDATE: Close current record and insert new version
        
        -- Step 1: Close the current record
        UPDATE mdm_vendor_scd2
        SET effective_end_date = DATE_SUB(NOW(), INTERVAL 1 SECOND),
            is_current = FALSE,
            updated_at = NOW(),
            updated_by = p_changed_by
        WHERE vendor_sk = v_existing_sk;
        
        -- Step 2: Insert new version
        INSERT INTO mdm_vendor_scd2 (
            source_vendor_id, vendor_name, lifecycle_state, match_confidence,
            source_system, effective_start_date, effective_end_date, is_current,
            version_number, created_by, change_reason, previous_vendor_name
        ) VALUES (
            p_source_vendor_id, p_vendor_name, p_lifecycle_state, p_match_confidence,
            p_source_system, NOW(), '9999-12-31 23:59:59', TRUE,
            v_current_version + 1, p_changed_by, p_change_reason, v_existing_name
        );
    END IF;
    -- No action if record exists but no changes
    
END //

-- ============================================================================
-- STORED PROCEDURE: Point-in-Time Query
-- ============================================================================
DELIMITER //
CREATE PROCEDURE sp_get_vendor_at_point_in_time(
    IN p_source_vendor_id INT,
    IN p_as_of_date DATETIME
)
BEGIN
    SELECT 
        vendor_sk,
        source_vendor_id,
        vendor_name,
        lifecycle_state,
        match_confidence,
        effective_start_date,
        effective_end_date,
        version_number,
        is_current
    FROM mdm_vendor_scd2
    WHERE source_vendor_id = p_source_vendor_id
      AND p_as_of_date >= effective_start_date
      AND p_as_of_date < effective_end_date;
END //

-- ============================================================================
-- STORED PROCEDURE: Get Complete History
-- ============================================================================
DELIMITER //
CREATE PROCEDURE sp_get_vendor_history(
    IN p_source_vendor_id INT
)
BEGIN
    SELECT 
        vendor_sk,
        source_vendor_id,
        vendor_name,
        lifecycle_state,
        match_confidence,
        effective_start_date,
        effective_end_date,
        version_number,
        is_current,
        previous_vendor_name,
        created_by,
        change_reason
    FROM mdm_vendor_scd2
    WHERE source_vendor_id = p_source_vendor_id
    ORDER BY version_number ASC;
END //

-- ============================================================================
-- STORED PROCEDURE: Rollback to Previous Version
-- ============================================================================
DELIMITER //
CREATE PROCEDURE sp_rollback_vendor(
    IN p_source_vendor_id INT,
    IN p_rollback_by VARCHAR(50),
    IN p_rollback_reason TEXT,
    OUT p_success BOOLEAN,
    OUT p_message VARCHAR(255)
)
BEGIN
    DECLARE v_current_sk BIGINT;
    DECLARE v_previous_sk BIGINT;
    DECLARE v_prev_name VARCHAR(200);
    DECLARE v_prev_state VARCHAR(20);
    DECLARE v_prev_confidence DECIMAL(5,2);
    DECLARE v_current_version INT;
    
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SET p_success = FALSE;
        SET p_message = 'Database error during rollback';
        ROLLBACK;
    END;
    
    START TRANSACTION;
    
    -- Get current version
    SELECT vendor_sk, version_number
    INTO v_current_sk, v_current_version
    FROM mdm_vendor_scd2
    WHERE source_vendor_id = p_source_vendor_id AND is_current = TRUE;
    
    IF v_current_version <= 1 THEN
        SET p_success = FALSE;
        SET p_message = 'Cannot rollback - this is the first version';
        ROLLBACK;
    ELSE
        -- Get previous version data
        SELECT vendor_sk, vendor_name, lifecycle_state, match_confidence
        INTO v_previous_sk, v_prev_name, v_prev_state, v_prev_confidence
        FROM mdm_vendor_scd2
        WHERE source_vendor_id = p_source_vendor_id
          AND version_number = v_current_version - 1;
        
        -- Close current record
        UPDATE mdm_vendor_scd2
        SET effective_end_date = DATE_SUB(NOW(), INTERVAL 1 SECOND),
            is_current = FALSE,
            updated_at = NOW(),
            updated_by = p_rollback_by
        WHERE vendor_sk = v_current_sk;
        
        -- Insert rollback version
        INSERT INTO mdm_vendor_scd2 (
            source_vendor_id, vendor_name, lifecycle_state, match_confidence,
            source_system, effective_start_date, is_current, version_number,
            created_by, change_reason
        )
        SELECT 
            source_vendor_id, vendor_name, lifecycle_state, match_confidence,
            source_system, NOW(), TRUE, v_current_version + 1,
            p_rollback_by, CONCAT('ROLLBACK: ', p_rollback_reason)
        FROM mdm_vendor_scd2
        WHERE vendor_sk = v_previous_sk;
        
        SET p_success = TRUE;
        SET p_message = CONCAT('Rolled back to version ', v_current_version - 1);
        COMMIT;
    END IF;
    
END //

DELIMITER ;

-- ============================================================================
-- SCD TYPE 2 IMPLEMENTATION FOR ZONES
-- ============================================================================

DROP TABLE IF EXISTS mdm_zone_scd2;

CREATE TABLE mdm_zone_scd2 (
    zone_sk BIGINT AUTO_INCREMENT PRIMARY KEY,
    source_zone_id INT NOT NULL,
    zone_name VARCHAR(200) NOT NULL,
    borough VARCHAR(100),
    service_zone VARCHAR(100),
    lifecycle_state VARCHAR(20) NOT NULL DEFAULT 'PROPOSED',
    match_confidence DECIMAL(5,2),
    source_system VARCHAR(50),
    
    -- SCD Type 2 columns
    effective_start_date DATETIME NOT NULL,
    effective_end_date DATETIME DEFAULT '9999-12-31 23:59:59',
    is_current BOOLEAN DEFAULT TRUE,
    version_number INT DEFAULT 1,
    
    -- Audit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(50) NOT NULL,
    change_reason TEXT,
    
    INDEX idx_source_zone (source_zone_id),
    INDEX idx_current (is_current),
    INDEX idx_effective (effective_start_date, effective_end_date),
    UNIQUE KEY uk_zone_current (source_zone_id, is_current, effective_end_date)
);

-- ============================================================================
-- BITEMPORAL TABLE (Transaction Time + Valid Time)
-- ============================================================================

DROP TABLE IF EXISTS mdm_vendor_bitemporal;

CREATE TABLE mdm_vendor_bitemporal (
    vendor_btk BIGINT AUTO_INCREMENT PRIMARY KEY,
    source_vendor_id INT NOT NULL,
    vendor_name VARCHAR(200) NOT NULL,
    lifecycle_state VARCHAR(20) NOT NULL,
    
    -- Valid time (business time - when the fact was true in the real world)
    valid_from DATETIME NOT NULL,
    valid_to DATETIME DEFAULT '9999-12-31 23:59:59',
    
    -- Transaction time (system time - when recorded in database)
    transaction_from DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    transaction_to DATETIME DEFAULT '9999-12-31 23:59:59',
    
    -- Audit
    created_by VARCHAR(50) NOT NULL,
    change_reason TEXT,
    
    INDEX idx_source (source_vendor_id),
    INDEX idx_valid_time (valid_from, valid_to),
    INDEX idx_transaction_time (transaction_from, transaction_to)
);

-- ============================================================================
-- SAMPLE DATA LOADING
-- ============================================================================

-- Initial load from existing mdm_vendor
INSERT INTO mdm_vendor_scd2 (
    source_vendor_id, vendor_name, lifecycle_state, match_confidence,
    source_system, effective_start_date, created_by, change_reason
)
SELECT 
    source_vendor_id,
    vendor_name,
    lifecycle_state,
    match_confidence,
    source_system,
    COALESCE(created_at, NOW()),
    COALESCE(created_by, 'migration'),
    'Initial SCD2 migration'
FROM mdm_vendor
WHERE lifecycle_state = 'ACTIVE';

-- ============================================================================
-- EXAMPLE QUERIES
-- ============================================================================

-- Get current state of all vendors
-- SELECT * FROM mdm_vendor_scd2;

-- Get vendor as of specific date
-- CALL sp_get_vendor_at_point_in_time(1, '2024-06-15 00:00:00');

-- Get complete history
-- CALL sp_get_vendor_history(1);

-- Simulate a change
-- CALL sp_upsert_vendor_scd2(1, 'Creative Mobile Tech LLC', 'ACTIVE', 100.00, 'NYC_TLC', 'admin', 'Name correction');

-- Rollback if needed
-- CALL sp_rollback_vendor(1, 'admin', 'Incorrect change', @success, @msg);
-- SELECT @success, @msg;