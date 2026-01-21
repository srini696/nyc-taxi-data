-- ============================================================================
-- SCD TYPE 1 & TYPE 3 IMPLEMENTATIONS
-- Complete implementation with comparison to Type 2
-- ============================================================================

USE nyc_taxi_mdm;

-- ============================================================================
-- PART 1: SCD TYPE 1 (OVERWRITE - NO HISTORY)
-- ============================================================================

DROP TABLE IF EXISTS mdm_vendor_type1;
CREATE TABLE mdm_vendor_type1 (
  vendor_id         INT AUTO_INCREMENT PRIMARY KEY,
  source_vendor_id  INT UNIQUE,
  vendor_name       VARCHAR(200) NOT NULL,
  lifecycle_state   VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
  source_system     VARCHAR(50),
  last_updated      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  updated_by        VARCHAR(50)
);

DELIMITER //
DROP PROCEDURE IF EXISTS sp_upsert_vendor_type1//
CREATE PROCEDURE sp_upsert_vendor_type1(
  IN p_source_vendor_id INT,
  IN p_vendor_name VARCHAR(200),
  IN p_source_system VARCHAR(50),
  IN p_updated_by VARCHAR(50),
  OUT p_vendor_id INT,
  OUT p_operation VARCHAR(20)
)
BEGIN
  DECLARE v_existing_id INT;

  -- Initialize local variable
  SET v_existing_id = NULL;

  -- Check if exists (expect max 1 row because source_vendor_id is UNIQUE)
  SELECT vendor_id
    INTO v_existing_id
  FROM mdm_vendor_type1
  WHERE source_vendor_id = p_source_vendor_id
  LIMIT 1;

  IF v_existing_id IS NULL THEN
    -- INSERT new record
    INSERT INTO mdm_vendor_type1 (
      source_vendor_id, vendor_name, source_system, updated_by
    )
    VALUES (
      p_source_vendor_id, p_vendor_name, p_source_system, p_updated_by
    );

    SET p_vendor_id = LAST_INSERT_ID();
    SET p_operation = 'INSERT';
  ELSE
    -- UPDATE existing record (overwrite, no history)
    UPDATE mdm_vendor_type1
    SET 
      vendor_name   = p_vendor_name,
      source_system = p_source_system,
      updated_by    = p_updated_by
    WHERE vendor_id = v_existing_id;

    SET p_vendor_id = v_existing_id;
    SET p_operation = 'UPDATE';
  END IF;
END//
DELIMITER ;

-- ============================================================================
-- PART 2: SCD TYPE 3 (LIMITED HISTORY - PREVIOUS VALUE COLUMN)
-- ============================================================================

DROP TABLE IF EXISTS mdm_vendor_type3;
CREATE TABLE mdm_vendor_type3 (
  vendor_id             INT AUTO_INCREMENT PRIMARY KEY,
  source_vendor_id      INT UNIQUE,

  -- Current values
  vendor_name           VARCHAR(200) NOT NULL,
  lifecycle_state       VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',

  -- Previous values (Type 3 specific)
  previous_vendor_name  VARCHAR(200),
  name_changed_date     TIMESTAMP NULL,

  -- Audit columns
  source_system         VARCHAR(50),
  created_at            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  created_by            VARCHAR(50),
  last_updated          TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  updated_by            VARCHAR(50)
);

DELIMITER //
DROP PROCEDURE IF EXISTS sp_upsert_vendor_type3//
CREATE PROCEDURE sp_upsert_vendor_type3(
  IN p_source_vendor_id INT,
  IN p_vendor_name VARCHAR(200),
  IN p_source_system VARCHAR(50),
  IN p_updated_by VARCHAR(50),
  OUT p_vendor_id INT,
  OUT p_operation VARCHAR(20)
)
BEGIN
  DECLARE v_existing_id INT;
  DECLARE v_current_name VARCHAR(200);

  SET v_existing_id = NULL;
  SET v_current_name = NULL;

  -- Check if exists
  SELECT vendor_id, vendor_name
    INTO v_existing_id, v_current_name
  FROM mdm_vendor_type3
  WHERE source_vendor_id = p_source_vendor_id
  LIMIT 1;

  IF v_existing_id IS NULL THEN
    -- INSERT new record
    INSERT INTO mdm_vendor_type3 (
      source_vendor_id, vendor_name, source_system,
      created_by, updated_by
    )
    VALUES (
      p_source_vendor_id, p_vendor_name, p_source_system,
      p_updated_by, p_updated_by
    );

    SET p_vendor_id = LAST_INSERT_ID();
    SET p_operation = 'INSERT';

  ELSEIF v_current_name <> p_vendor_name THEN
    -- UPDATE: Move current to previous, set new as current
    UPDATE mdm_vendor_type3
    SET 
      previous_vendor_name = vendor_name,
      vendor_name          = p_vendor_name,
      name_changed_date    = CURRENT_TIMESTAMP,
      updated_by           = p_updated_by
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

-- ============================================================================
-- PART 3: ENHANCED TYPE 3 WITH MULTIPLE PREVIOUS VALUES
-- ============================================================================

DROP TABLE IF EXISTS mdm_vendor_type3_extended;
CREATE TABLE mdm_vendor_type3_extended (
  vendor_id                 INT AUTO_INCREMENT PRIMARY KEY,
  source_vendor_id          INT UNIQUE,

  -- Current values
  vendor_name               VARCHAR(200) NOT NULL,
  contact_email             VARCHAR(200),
  service_area              VARCHAR(100),
  lifecycle_state           VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',

  -- Previous values (Type 3 tracking)
  previous_vendor_name      VARCHAR(200),
  previous_contact_email    VARCHAR(200),
  previous_service_area     VARCHAR(100),

  -- Change tracking
  name_changed_date         TIMESTAMP NULL,
  email_changed_date        TIMESTAMP NULL,
  area_changed_date         TIMESTAMP NULL,

  -- Audit
  source_system             VARCHAR(50),
  created_at                TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  created_by                VARCHAR(50),
  last_updated              TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  updated_by                VARCHAR(50)
);

DELIMITER //
DROP PROCEDURE IF EXISTS sp_upsert_vendor_type3_extended//
CREATE PROCEDURE sp_upsert_vendor_type3_extended(
  IN p_source_vendor_id INT,
  IN p_vendor_name VARCHAR(200),
  IN p_contact_email VARCHAR(200),
  IN p_service_area VARCHAR(100),
  IN p_updated_by VARCHAR(50),
  OUT p_vendor_id INT,
  OUT p_operation VARCHAR(20)
)
BEGIN
  DECLARE v_existing_id INT;
  DECLARE v_current_name  VARCHAR(200);
  DECLARE v_current_email VARCHAR(200);
  DECLARE v_current_area  VARCHAR(100);

  SET v_existing_id = NULL;
  SET v_current_name = NULL;
  SET v_current_email = NULL;
  SET v_current_area = NULL;

  -- Check if exists
  SELECT vendor_id, vendor_name, contact_email, service_area
    INTO v_existing_id, v_current_name, v_current_email, v_current_area
  FROM mdm_vendor_type3_extended
  WHERE source_vendor_id = p_source_vendor_id
  LIMIT 1;

  IF v_existing_id IS NULL THEN
    -- INSERT
    INSERT INTO mdm_vendor_type3_extended (
      source_vendor_id, vendor_name, contact_email, service_area,
      created_by, updated_by
    )
    VALUES (
      p_source_vendor_id, p_vendor_name, p_contact_email, p_service_area,
      p_updated_by, p_updated_by
    );

    SET p_vendor_id = LAST_INSERT_ID();
    SET p_operation = 'INSERT';
  ELSE
    -- UPDATE with selective Type 3 tracking
    UPDATE mdm_vendor_type3_extended
    SET
      -- Update name if changed
      previous_vendor_name = IF(vendor_name <> p_vendor_name, vendor_name, previous_vendor_name),
      vendor_name          = p_vendor_name,
      name_changed_date    = IF(vendor_name <> p_vendor_name, CURRENT_TIMESTAMP, name_changed_date),

      -- Update email if changed
      previous_contact_email = IF(contact_email <> p_contact_email, contact_email, previous_contact_email),
      contact_email          = p_contact_email,
      email_changed_date     = IF(contact_email <> p_contact_email, CURRENT_TIMESTAMP, email_changed_date),

      -- Update area if changed
      previous_service_area = IF(service_area <> p_service_area, service_area, previous_service_area),
      service_area          = p_service_area,
      area_changed_date     = IF(service_area <> p_service_area, CURRENT_TIMESTAMP, area_changed_date),

      updated_by            = p_updated_by
    WHERE vendor_id = v_existing_id;

    SET p_vendor_id = v_existing_id;
    SET p_operation = 'UPDATE';
  END IF;
END//
DELIMITER ;

-- ============================================================================
-- PART 4: HYBRID APPROACH (Type 1 + Type 3)
-- ============================================================================

DROP TABLE IF EXISTS mdm_vendor_hybrid;
CREATE TABLE mdm_vendor_hybrid (
  vendor_id             INT AUTO_INCREMENT PRIMARY KEY,
  source_vendor_id      INT UNIQUE,

  -- Type 1 fields
  vendor_status         VARCHAR(20),
  last_audit_date       DATE,
  compliance_flag       BOOLEAN,

  -- Type 3 fields
  vendor_name           VARCHAR(200) NOT NULL,
  previous_vendor_name  VARCHAR(200),
  name_changed_date     TIMESTAMP NULL,

  contact_email         VARCHAR(200),
  previous_email        VARCHAR(200),
  email_changed_date    TIMESTAMP NULL,

  -- Audit
  source_system         VARCHAR(50),
  created_at            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  last_updated          TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  updated_by            VARCHAR(50)
);

-- ============================================================================
-- PART 5: COMPARISON QUERIES
-- ============================================================================

CREATE OR REPLACE VIEW vw_scd_comparison AS
SELECT 
  'TYPE_1' AS scd_type,
  'Overwrite' AS strategy,
  'No' AS maintains_history,
  'None' AS history_depth,
  'Simple updates' AS complexity,
  'Data corrections' AS best_use_case
UNION ALL
SELECT 
  'TYPE_2',
  'New row per change',
  'Yes',
  'Complete',
  'Moderate (history table)',
  'Audit trails, compliance'
UNION ALL
SELECT 
  'TYPE_3',
  'Previous value column',
  'Partial',
  'One previous value',
  'Simple',
  'Compare current vs previous';

CREATE OR REPLACE VIEW vw_type3_changes AS
SELECT 
  vendor_id,
  source_vendor_id,
  vendor_name AS current_name,
  previous_vendor_name,
  name_changed_date,
  DATEDIFF(CURRENT_TIMESTAMP, name_changed_date) AS days_since_change
FROM mdm_vendor_type3
WHERE previous_vendor_name IS NOT NULL
ORDER BY name_changed_date DESC;



USE nyc_taxi_mdm;

-- OUT parameters
SET @v_id  := NULL;
SET @v_op  := NULL;

-- 1) Initial load from your vendors.csv record with source_vendor_id = 1
CALL sp_upsert_vendor_type1(
  1,                                          -- p_source_vendor_id
  'Creative Mobile Technologies, LLC',        -- p_vendor_name
  'NYC_TLC_dictionary',                       -- p_source_system
  'initial_load',                             -- p_updated_by
  @v_id,
  @v_op
);
SELECT @v_id AS vendor_id, @v_op AS operation;

-- 2) Correct the name spelling (Type 1 overwrite – no history)
SET @v_id := NULL;
SET @v_op := NULL;

CALL sp_upsert_vendor_type1(
  1,
  'Creative Mobile Technologies LLC',         -- removed comma as a "correction"
  'NYC_TLC_dictionary',
  'data_steward',
  @v_id,
  @v_op
);
SELECT @v_id AS vendor_id, @v_op AS operation;

-- 3) Insert a brand‑new vendor not in the CSV (e.g., source_vendor_id = 60000)
SET @v_id := NULL;
SET @v_op := NULL;

CALL sp_upsert_vendor_type1(
  60000,
  'New Platform Vendor',
  'source_manual_20260113',
  'steward_test',
  @v_id,
  @v_op
);
SELECT @v_id AS vendor_id, @v_op AS operation;

-- Inspect all Type 1 records
SELECT * FROM mdm_vendor_type1 ORDER BY vendor_id;

USE nyc_taxi_mdm;

-- OUT parameters
SET @v_id  := NULL;
SET @v_op  := NULL;

-- 1) Seed with one of your active vendors (e.g., source_vendor_id = 999)
CALL sp_upsert_vendor_type3(
  999,
  'Myle Tech Inc',            
  'source_20260111',         
  'step_functions',           
  @v_id,
  @v_op
);
SELECT @v_id AS vendor_id, @v_op AS operation;

-- 2) Change the vendor name to simulate a rebranding (Type 3: track previous)
SET @v_id := NULL;
SET @v_op := NULL;

CALL sp_upsert_vendor_type3(
  999,
  'Myle Technologies Inc',    -- new name
  'source_20260111',
  'steward_test',
  @v_id,
  @v_op
);
SELECT @v_id AS vendor_id, @v_op AS operation;

-- 3) Call again with the same name (no change should be recorded)
SET @v_id := NULL;
SET @v_op := NULL;

CALL sp_upsert_vendor_type3(
  999,
  'Myle Technologies Inc',    -- same as current_name, so NO_CHANGE
  'source_20260111',
  'steward_test',
  @v_id,
  @v_op
);
SELECT @v_id AS vendor_id, @v_op AS operation;

-- Inspect the Type 3 tracking for this vendor
SELECT
  vendor_id,
  source_vendor_id,
  previous_vendor_name,
  vendor_name AS current_vendor_name,
  name_changed_date
FROM mdm_vendor_type3
WHERE source_vendor_id = 999;

USE nyc_taxi_mdm;

-- OUT parameters
SET @v_id  := NULL;
SET @v_op  := NULL;

-- 1) Insert a new vendor with email and service area
CALL sp_upsert_vendor_type3_extended(
  888,                            -- re‑using one of your CSV source_vendor_id values
  'Myle Tech Inc',                -- vendor_name
  'contact@myletech.com',         -- contact_email
  'Manhattan',                    -- service_area
  'step_functions',               -- updated_by / created_by
  @v_id,
  @v_op
);
SELECT @v_id AS vendor_id, @v_op AS operation;

-- 2) Change only the email (tracks previous_contact_email + email_changed_date)
SET @v_id := NULL;
SET @v_op := NULL;

CALL sp_upsert_vendor_type3_extended(
  888,
  'Myle Tech Inc',                -- same name
  'support@myletech.com',         -- new email
  'Manhattan',                    -- same area
  'steward_test',
  @v_id,
  @v_op
);
SELECT @v_id AS vendor_id, @v_op AS operation;

-- 3) Change name and area simultaneously (tracks all three “previous” columns)
SET @v_id := NULL;
SET @v_op := NULL;

CALL sp_upsert_vendor_type3_extended(
  888,
  'Myle Technologies Inc',        -- new name
  'support@myletech.com',         -- same email
  'New York Metro',               -- new area
  'steward_test',
  @v_id,
  @v_op
);
SELECT @v_id AS vendor_id, @v_op AS operation;

-- Inspect the extended tracking
SELECT
  vendor_id,
  source_vendor_id,
  previous_vendor_name,
  vendor_name AS current_name,
  previous_contact_email,
  contact_email AS current_email,
  previous_service_area,
  service_area AS current_area,
  name_changed_date,
  email_changed_date,
  area_changed_date
FROM mdm_vendor_type3_extended
WHERE source_vendor_id = 888;

USE nyc_taxi_mdm;

-- 1) Insert a new hybrid vendor using one of your CSV sources (e.g., 55555)
INSERT INTO mdm_vendor_hybrid (
  source_vendor_id,
  vendor_status,
  last_audit_date,
  compliance_flag,
  vendor_name,
  contact_email,
  source_system,
  updated_by
)
VALUES (
  55555,
  'ACTIVE',
  '2026-01-13',
  TRUE,
  'Creative Mobile Technologies, LLC',
  'compliance@creativemobile.com',
  'source_exact_match_20260111',
  'initial_load'
);

-- 2) Update Type 1‑style fields only (status and compliance overwrite)
UPDATE mdm_vendor_hybrid
SET
  vendor_status   = 'SUSPENDED',
  last_audit_date = '2026-02-01',
  compliance_flag = FALSE,
  updated_by      = 'audit_team'
WHERE source_vendor_id = 55555;

-- 3) Simulate a Type 3‑style change for the name and email
UPDATE mdm_vendor_hybrid
SET
  previous_vendor_name = vendor_name,
  vendor_name          = 'Creative Mobility Technologies',
  name_changed_date    = CURRENT_TIMESTAMP,
  previous_email       = contact_email,
  contact_email        = 'support@creativemobility.com',
  email_changed_date   = CURRENT_TIMESTAMP,
  updated_by           = 'data_steward'
WHERE source_vendor_id = 55555;

-- Inspect hybrid data
SELECT * FROM mdm_vendor_hybrid WHERE source_vendor_id = 55555;
