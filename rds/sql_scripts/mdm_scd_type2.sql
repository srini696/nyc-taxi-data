
USE nyc_taxi_mdm;


-- CHANGE ID GENERATOR (STORED PROCEDURE VERSION)

DROP TABLE IF EXISTS mdm_change_sequence;
CREATE TABLE mdm_change_sequence (
  last_change_id BIGINT UNSIGNED NOT NULL DEFAULT 0
);
INSERT INTO mdm_change_sequence VALUES (0);

DELIMITER //
DROP PROCEDURE IF EXISTS sp_get_next_change_id//
CREATE PROCEDURE sp_get_next_change_id(
  OUT p_change_id BIGINT UNSIGNED
)
BEGIN
  UPDATE mdm_change_sequence SET last_change_id = last_change_id + 1;
  SELECT last_change_id INTO p_change_id FROM mdm_change_sequence;
END//
DELIMITER ;


--  VENDOR SCD TYPE 2 STORED PROCEDURE


DELIMITER //
DROP PROCEDURE IF EXISTS sp_upsert_vendor_scd2//
CREATE PROCEDURE sp_upsert_vendor_scd2(
  IN p_source_vendor_id INT,
  IN p_vendor_name VARCHAR(200),
  IN p_source_system VARCHAR(50),
  IN p_created_by VARCHAR(50),
  IN p_match_confidence DECIMAL(5,2),
  OUT p_vendor_id INT,
  OUT p_operation VARCHAR(20)
)
BEGIN
  DECLARE v_existing_id INT DEFAULT NULL;
  DECLARE v_existing_name VARCHAR(200) DEFAULT NULL;
  DECLARE v_change_id BIGINT UNSIGNED DEFAULT NULL;
  DECLARE v_lifecycle VARCHAR(20) DEFAULT NULL;
  
  -- Get change_id for this batch
  CALL sp_get_next_change_id(v_change_id);
  
  -- Check if vendor exists
  SELECT vendor_id, vendor_name, lifecycle_state 
  INTO v_existing_id, v_existing_name, v_lifecycle
  FROM mdm_vendor 
  WHERE source_vendor_id = p_source_vendor_id
  LIMIT 1;
  
  IF v_existing_id IS NULL THEN
    -- INSERT: New vendor
    INSERT INTO mdm_vendor (
      source_vendor_id, vendor_name, lifecycle_state,
      match_confidence, source_system, created_by, change_id
    ) VALUES (
      p_source_vendor_id, p_vendor_name, 'PROPOSED',
      p_match_confidence, p_source_system, p_created_by, v_change_id
    );
    
    SET p_vendor_id = LAST_INSERT_ID();
    SET p_operation = 'INSERT';
    
    -- Log to history
    INSERT INTO mdm_vendor_history (
      change_id, vendor_id, vendor_name, lifecycle_state,
      match_confidence, source_system, created_by, change_type, change_reason
    ) VALUES (
      v_change_id, p_vendor_id, p_vendor_name, 'PROPOSED',
      p_match_confidence, p_source_system, p_created_by, 'INSERT', 'New vendor added'
    );
    
  ELSEIF v_existing_name != p_vendor_name THEN
    -- UPDATE: Vendor name changed - SCD Type 2
    -- Archive old version to history
    INSERT INTO mdm_vendor_history (
      change_id, vendor_id, vendor_name, lifecycle_state,
      match_confidence, source_system, created_by, approved_at, approved_by,
      change_type, change_reason
    )
    SELECT 
      v_change_id, vendor_id, vendor_name, lifecycle_state,
      match_confidence, source_system, created_by, approved_at, approved_by,
      'UPDATE', CONCAT('Name changed from "', v_existing_name, '" to "', p_vendor_name, '"')
    FROM mdm_vendor
    WHERE vendor_id = v_existing_id;
    
    -- Update current record
    UPDATE mdm_vendor
    SET 
      vendor_name = p_vendor_name,
      match_confidence = p_match_confidence,
      updated_by = p_created_by,
      updated_at = CURRENT_TIMESTAMP,
      change_id = v_change_id,
      lifecycle_state = IF(lifecycle_state = 'ACTIVE', 'ACTIVE', 'PROPOSED')
    WHERE vendor_id = v_existing_id;
    
    SET p_vendor_id = v_existing_id;
    SET p_operation = 'UPDATE';
    
  ELSE
    -- NO CHANGE
    SET p_vendor_id = v_existing_id;
    SET p_operation = 'NO_CHANGE';
  END IF;
  
END//
DELIMITER ;


-- RATECODE SCD TYPE 2 STORED PROCEDURE


DELIMITER //
DROP PROCEDURE IF EXISTS sp_upsert_ratecode_scd2//
CREATE PROCEDURE sp_upsert_ratecode_scd2(
  IN p_source_ratecode_id INT,
  IN p_rate_code_desc VARCHAR(100),
  IN p_source_system VARCHAR(50),
  IN p_created_by VARCHAR(50),
  IN p_match_confidence DECIMAL(5,2),
  OUT p_ratecode_id INT,
  OUT p_operation VARCHAR(20)
)
BEGIN
  DECLARE v_existing_id INT DEFAULT NULL;
  DECLARE v_existing_desc VARCHAR(100) DEFAULT NULL;
  DECLARE v_change_id BIGINT UNSIGNED DEFAULT NULL;
  
  CALL sp_get_next_change_id(v_change_id);
  
  SELECT ratecode_id, rate_code_desc 
  INTO v_existing_id, v_existing_desc
  FROM mdm_ratecode 
  WHERE source_ratecode_id = p_source_ratecode_id
  LIMIT 1;
  
  IF v_existing_id IS NULL THEN
    INSERT INTO mdm_ratecode (
      source_ratecode_id, rate_code_desc, lifecycle_state,
      match_confidence, source_system, created_by, change_id
    ) VALUES (
      p_source_ratecode_id, p_rate_code_desc, 'PROPOSED',
      p_match_confidence, p_source_system, p_created_by, v_change_id
    );
    
    SET p_ratecode_id = LAST_INSERT_ID();
    SET p_operation = 'INSERT';
    
    INSERT INTO mdm_ratecode_history (
      change_id, ratecode_id, rate_code_desc, lifecycle_state,
      match_confidence, source_system, created_by, change_type, change_reason
    ) VALUES (
      v_change_id, p_ratecode_id, p_rate_code_desc, 'PROPOSED',
      p_match_confidence, p_source_system, p_created_by, 'INSERT', 'New rate code added'
    );
    
  ELSEIF v_existing_desc != p_rate_code_desc THEN
    INSERT INTO mdm_ratecode_history (
      change_id, ratecode_id, rate_code_desc, lifecycle_state,
      match_confidence, source_system, created_by, approved_at, approved_by,
      change_type, change_reason
    )
    SELECT 
      v_change_id, ratecode_id, rate_code_desc, lifecycle_state,
      match_confidence, source_system, created_by, approved_at, approved_by,
      'UPDATE', CONCAT('Description changed from "', v_existing_desc, '" to "', p_rate_code_desc, '"')
    FROM mdm_ratecode
    WHERE ratecode_id = v_existing_id;
    
    UPDATE mdm_ratecode
    SET 
      rate_code_desc = p_rate_code_desc,
      match_confidence = p_match_confidence,
      updated_by = p_created_by,
      updated_at = CURRENT_TIMESTAMP,
      change_id = v_change_id
    WHERE ratecode_id = v_existing_id;
    
    SET p_ratecode_id = v_existing_id;
    SET p_operation = 'UPDATE';
  ELSE
    SET p_ratecode_id = v_existing_id;
    SET p_operation = 'NO_CHANGE';
  END IF;
END//
DELIMITER ;


--  ZONE SCD TYPE 2 STORED PROCEDURE


DELIMITER //
DROP PROCEDURE IF EXISTS sp_upsert_zone_scd2//
CREATE PROCEDURE sp_upsert_zone_scd2(
  IN p_source_zone_id INT,
  IN p_zone_name VARCHAR(200),
  IN p_source_system VARCHAR(50),
  IN p_created_by VARCHAR(50),
  IN p_match_confidence DECIMAL(5,2),
  OUT p_zone_id INT,
  OUT p_operation VARCHAR(20)
)
BEGIN
  DECLARE v_existing_id INT DEFAULT NULL;
  DECLARE v_existing_name VARCHAR(200) DEFAULT NULL;
  DECLARE v_change_id BIGINT UNSIGNED DEFAULT NULL;
  
  CALL sp_get_next_change_id(v_change_id);
  
  SELECT zone_id, zone_name 
  INTO v_existing_id, v_existing_name
  FROM mdm_zone 
  WHERE source_zone_id = p_source_zone_id
  LIMIT 1;
  
  IF v_existing_id IS NULL THEN
    INSERT INTO mdm_zone (
      source_zone_id, zone_name, lifecycle_state,
      match_confidence, source_system, created_by, change_id
    ) VALUES (
      p_source_zone_id, p_zone_name, 'PROPOSED',
      p_match_confidence, p_source_system, p_created_by, v_change_id
    );
    
    SET p_zone_id = LAST_INSERT_ID();
    SET p_operation = 'INSERT';
    
    INSERT INTO mdm_zone_history (
      change_id, zone_id, zone_name, lifecycle_state,
      match_confidence, source_system, created_by, change_type, change_reason
    ) VALUES (
      v_change_id, p_zone_id, p_zone_name, 'PROPOSED',
      p_match_confidence, p_source_system, p_created_by, 'INSERT', 'New zone added'
    );
    
  ELSEIF v_existing_name != p_zone_name THEN
    INSERT INTO mdm_zone_history (
      change_id, zone_id, zone_name, lifecycle_state,
      match_confidence, source_system, created_by, approved_at, approved_by,
      change_type, change_reason
    )
    SELECT 
      v_change_id, zone_id, zone_name, lifecycle_state,
      match_confidence, source_system, created_by, approved_at, approved_by,
      'UPDATE', CONCAT('Name changed from "', v_existing_name, '" to "', p_zone_name, '"')
    FROM mdm_zone
    WHERE zone_id = v_existing_id;
    
    UPDATE mdm_zone
    SET 
      zone_name = p_zone_name,
      match_confidence = p_match_confidence,
      updated_by = p_created_by,
      updated_at = CURRENT_TIMESTAMP,
      change_id = v_change_id
    WHERE zone_id = v_existing_id;
    
    SET p_zone_id = v_existing_id;
    SET p_operation = 'UPDATE';
  ELSE
    SET p_zone_id = v_existing_id;
    SET p_operation = 'NO_CHANGE';
  END IF;
END//
DELIMITER ;


--  APPROVAL WORKFLOW PROCEDURE


DELIMITER //
DROP PROCEDURE IF EXISTS sp_approve_pending_records//
CREATE PROCEDURE sp_approve_pending_records(
  IN p_entity_type VARCHAR(20),
  IN p_approved_by VARCHAR(50),
  IN p_batch_size INT
)
BEGIN
  DECLARE v_rows_affected INT DEFAULT 0;
  
  IF p_entity_type = 'VENDOR' THEN
    UPDATE mdm_vendor
    SET 
      lifecycle_state = 'ACTIVE',
      approved_at = CURRENT_TIMESTAMP,
      approved_by = p_approved_by
    WHERE lifecycle_state = 'PROPOSED'
    LIMIT p_batch_size;
    
    SET v_rows_affected = ROW_COUNT();
    
  ELSEIF p_entity_type = 'RATECODE' THEN
    UPDATE mdm_ratecode
    SET 
      lifecycle_state = 'ACTIVE',
      approved_at = CURRENT_TIMESTAMP,
      approved_by = p_approved_by
    WHERE lifecycle_state = 'PROPOSED'
    LIMIT p_batch_size;
    
    SET v_rows_affected = ROW_COUNT();
    
  ELSEIF p_entity_type = 'ZONE' THEN
    UPDATE mdm_zone
    SET 
      lifecycle_state = 'ACTIVE',
      approved_at = CURRENT_TIMESTAMP,
      approved_by = p_approved_by
    WHERE lifecycle_state = 'PROPOSED'
    LIMIT p_batch_size;
    
    SET v_rows_affected = ROW_COUNT();
  END IF;
  
  SELECT v_rows_affected AS records_approved;
END//
DELIMITER ;


--  POINT-IN-TIME QUERY PROCEDURE


DELIMITER //
DROP PROCEDURE IF EXISTS sp_get_vendor_at_date//
CREATE PROCEDURE sp_get_vendor_at_date(
  IN p_vendor_id INT,
  IN p_as_of_date DATETIME
)
BEGIN
  DECLARE v_exists INT DEFAULT 0;
  
  -- Check if current record exists and was created before the target date
  SELECT COUNT(*) INTO v_exists
  FROM mdm_vendor 
  WHERE vendor_id = p_vendor_id 
  AND created_at <= p_as_of_date;
  
  IF v_exists > 0 THEN
    -- Return current record
    SELECT 
      vendor_id, 
      vendor_name, 
      lifecycle_state,
      match_confidence, 
      created_at, 
      approved_at,
      'CURRENT' AS source
    FROM mdm_vendor
    WHERE vendor_id = p_vendor_id
    AND created_at <= p_as_of_date;
  ELSE
    -- Look in history
    SELECT 
      vendor_id, 
      vendor_name, 
      lifecycle_state,
      match_confidence, 
      created_at, 
      approved_at,
      'HISTORY' AS source
    FROM mdm_vendor_history
    WHERE vendor_id = p_vendor_id
    AND created_at <= p_as_of_date
    ORDER BY created_at DESC
    LIMIT 1;
  END IF;
END//
DELIMITER ;

--ROLLBACK PROCEDURE


DELIMITER //
DROP PROCEDURE IF EXISTS sp_rollback_change//
CREATE PROCEDURE sp_rollback_change(
  IN p_change_id BIGINT UNSIGNED,
  IN p_entity_type VARCHAR(20)
)
BEGIN
  DECLARE v_count INT DEFAULT 0;
  DECLARE v_rollback_user VARCHAR(50);
  
  SET v_rollback_user = CONCAT('ROLLBACK_', USER());
  
  IF p_entity_type = 'VENDOR' THEN
    SELECT COUNT(*) INTO v_count
    FROM mdm_vendor_history
    WHERE change_id = p_change_id;
    
    IF v_count > 0 THEN
      UPDATE mdm_vendor v
      INNER JOIN (
        SELECT 
          vendor_id,
          vendor_name,
          lifecycle_state,
          match_confidence
        FROM mdm_vendor_history
        WHERE change_id = p_change_id
      ) h ON v.vendor_id = h.vendor_id
      SET 
        v.vendor_name = h.vendor_name,
        v.lifecycle_state = h.lifecycle_state,
        v.match_confidence = h.match_confidence,
        v.updated_by = v_rollback_user,
        v.updated_at = CURRENT_TIMESTAMP;
      
      SELECT CONCAT('Rolled back ', ROW_COUNT(), ' vendor records') AS result;
    ELSE
      SELECT 'No changes found for this change_id' AS result;
    END IF;
    
  ELSEIF p_entity_type = 'RATECODE' THEN
    SELECT COUNT(*) INTO v_count
    FROM mdm_ratecode_history
    WHERE change_id = p_change_id;
    
    IF v_count > 0 THEN
      UPDATE mdm_ratecode r
      INNER JOIN (
        SELECT 
          ratecode_id,
          rate_code_desc,
          lifecycle_state,
          match_confidence
        FROM mdm_ratecode_history
        WHERE change_id = p_change_id
      ) h ON r.ratecode_id = h.ratecode_id
      SET 
        r.rate_code_desc = h.rate_code_desc,
        r.lifecycle_state = h.lifecycle_state,
        r.match_confidence = h.match_confidence,
        r.updated_by = v_rollback_user,
        r.updated_at = CURRENT_TIMESTAMP;
      
      SELECT CONCAT('Rolled back ', ROW_COUNT(), ' ratecode records') AS result;
    ELSE
      SELECT 'No changes found for this change_id' AS result;
    END IF;
    
  ELSEIF p_entity_type = 'ZONE' THEN
    SELECT COUNT(*) INTO v_count
    FROM mdm_zone_history
    WHERE change_id = p_change_id;
    
    IF v_count > 0 THEN
      UPDATE mdm_zone z
      INNER JOIN (
        SELECT 
          zone_id,
          zone_name,
          lifecycle_state,
          match_confidence
        FROM mdm_zone_history
        WHERE change_id = p_change_id
      ) h ON z.zone_id = h.zone_id
      SET 
        z.zone_name = h.zone_name,
        z.lifecycle_state = h.lifecycle_state,
        z.match_confidence = h.match_confidence,
        z.updated_by = v_rollback_user,
        z.updated_at = CURRENT_TIMESTAMP;
      
      SELECT CONCAT('Rolled back ', ROW_COUNT(), ' zone records') AS result;
    ELSE
      SELECT 'No changes found for this change_id' AS result;
    END IF;
  END IF;
END//
DELIMITER ;

--  DATA QUALITY VALIDATION VIEWS


DROP VIEW IF EXISTS vw_dq_duplicate_vendors;
CREATE VIEW vw_dq_duplicate_vendors AS
SELECT 
  vendor_name,
  COUNT(*) AS duplicate_count,
  GROUP_CONCAT(vendor_id ORDER BY vendor_id) AS vendor_ids
FROM mdm_vendor
WHERE lifecycle_state = 'ACTIVE'
GROUP BY vendor_name
HAVING COUNT(*) > 1;

DROP VIEW IF EXISTS vw_dq_orphaned_trips;
CREATE VIEW vw_dq_orphaned_trips AS
SELECT 
  'VENDOR' AS entity_type,
  COUNT(*) AS orphaned_count
FROM mdm_golden_trip_level t
LEFT JOIN mdm_vendor v ON t.vendorid = v.source_vendor_id
WHERE v.vendor_id IS NULL AND t.vendorid IS NOT NULL
UNION ALL
SELECT 
  'RATECODE',
  COUNT(*)
FROM mdm_golden_trip_level t
LEFT JOIN mdm_ratecode r ON t.ratecodeid = r.source_ratecode_id
WHERE r.ratecode_id IS NULL AND t.ratecodeid IS NOT NULL;

DROP VIEW IF EXISTS vw_dq_audit_completeness;
CREATE VIEW vw_dq_audit_completeness AS
SELECT 
  'VENDOR' AS entity_type,
  COUNT(*) AS total_records,
  SUM(CASE WHEN approved_by IS NULL THEN 1 ELSE 0 END) AS missing_approver,
  SUM(CASE WHEN change_id IS NULL THEN 1 ELSE 0 END) AS missing_change_id
FROM mdm_vendor
WHERE lifecycle_state = 'ACTIVE'
UNION ALL
SELECT 
  'RATECODE',
  COUNT(*),
  SUM(CASE WHEN approved_by IS NULL THEN 1 ELSE 0 END),
  SUM(CASE WHEN change_id IS NULL THEN 1 ELSE 0 END)
FROM mdm_ratecode
WHERE lifecycle_state = 'ACTIVE'
UNION ALL
SELECT 
  'ZONE',
  COUNT(*),
  SUM(CASE WHEN approved_by IS NULL THEN 1 ELSE 0 END),
  SUM(CASE WHEN change_id IS NULL THEN 1 ELSE 0 END)
FROM mdm_zone
WHERE lifecycle_state = 'ACTIVE';



--  Insert/Update a vendor with SCD Type 2
CALL sp_upsert_vendor_scd2(
  989,                          -- source_vendor_id
  'Creative Mobile Tech LLC',  -- vendor_name
  'NYC_TLC_update',            -- source_system
  'etl_pipeline',              -- created_by
  98.50,                       -- match_confidence
  @vendor_id,                  -- OUT: vendor_id
  @operation                   -- OUT: operation type
);
SELECT @vendor_id AS vendor_id, @operation AS operation;

--  Update the same vendor (creates history)
CALL sp_upsert_vendor_scd2(
  989, 
  'Creative Mobile Technologies LLC',  -- Changed name
  'NYC_TLC_update', 
  'etl_pipeline', 
  98.50,
  @vendor_id, 
  @operation
);
SELECT @vendor_id AS vendor_id, @operation AS operation;

--  Check the history
SELECT * FROM mdm_vendor WHERE source_vendor_id = 989;
SELECT * FROM mdm_vendor_history WHERE vendor_id = @vendor_id;

--  Approve pending records
CALL sp_approve_pending_records('VENDOR', 'data_steward', 100);

--  Point-in-time query
CALL sp_get_vendor_at_date(1, '2024-01-01 00:00:00');

-- Check data quality
SELECT * FROM vw_dq_duplicate_vendors;
SELECT * FROM vw_dq_orphaned_trips;
SELECT * FROM vw_dq_audit_completeness;

--  Rollback a change
-- First get the change_id from history
SELECT change_id FROM mdm_vendor_history WHERE vendor_id = @vendor_id ORDER BY created_at DESC LIMIT 1;
-- Then rollback
CALL sp_rollback_change(12345, 'VENDOR');

--  Complete test workflow
SET @vid = NULL;
SET @vop = NULL;

-- Insert new vendor
CALL sp_upsert_vendor_scd2(999, 'Test Vendor Inc', 'test_system', 'test_user', 100.0, @vid, @vop);
SELECT CONCAT('Operation: ', @vop, ', ID: ', @vid) AS result;

-- Update the vendor (creates history)
CALL sp_upsert_vendor_scd2(999, 'Test Vendor Corporation', 'test_system', 'test_user', 100.0, @vid, @vop);
SELECT CONCAT('Operation: ', @vop, ', ID: ', @vid) AS result;

-- View current and history
SELECT 'CURRENT' AS type, vendor_id, vendor_name, lifecycle_state 
FROM mdm_vendor WHERE vendor_id = @vid
UNION ALL
SELECT 'HISTORY', vendor_id, vendor_name, lifecycle_state 
FROM mdm_vendor_history WHERE vendor_id = @vid;

-- Cleanup test data
DELETE FROM mdm_vendor WHERE source_vendor_id = 999;
DELETE FROM mdm_vendor_history WHERE vendor_id = @vid;
