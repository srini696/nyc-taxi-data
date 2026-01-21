USE nyc_taxi_mdm;


CREATE TABLE IF NOT EXISTS mdm_vendor (
  vendor_id         INT AUTO_INCREMENT PRIMARY KEY,        
  source_vendor_id  INT UNIQUE,
  vendor_name       VARCHAR(200) NOT NULL,
  lifecycle_state   VARCHAR(20) NOT NULL DEFAULT 'PROPOSED',
  match_confidence  DECIMAL(5,2),
  source_system     VARCHAR(50),
  created_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  created_by        VARCHAR(50) NOT NULL,
  updated_at        TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  updated_by        VARCHAR(50),
  approved_at       TIMESTAMP NULL DEFAULT NULL,
  approved_by       VARCHAR(50),
  change_id         BIGINT UNSIGNED DEFAULT NULL           
);
CREATE TABLE IF NOT EXISTS mdm_vendor_history (
  history_id        INT AUTO_INCREMENT PRIMARY KEY,
  change_id         BIGINT UNSIGNED,
  vendor_id         INT,
  vendor_name       VARCHAR(200),
  lifecycle_state   VARCHAR(20),
  match_confidence  DECIMAL(5,2),
  source_system     VARCHAR(50),
  created_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  created_by        VARCHAR(50) NOT NULL,
  approved_at       TIMESTAMP NULL DEFAULT NULL,
  approved_by       VARCHAR(50),
  change_type       VARCHAR(20),
  change_reason     TEXT,
  INDEX idx_change_id (change_id),
  INDEX idx_vendor_id (vendor_id)
);

CREATE TABLE IF NOT EXISTS mdm_vendor_dedup_queue (
  queue_id          INT AUTO_INCREMENT PRIMARY KEY,         
  vendor_id_1       INT,
  vendor_id_2       INT,
  match_score       DECIMAL(5,2),                            
  confidence_level  VARCHAR(10),
  created_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  steward_decision  VARCHAR(20),
  decision_reason   TEXT,
  decided_at        TIMESTAMP NULL DEFAULT NULL,
  decided_by        VARCHAR(50),
  
 
  FOREIGN KEY (vendor_id_1) REFERENCES mdm_vendor(vendor_id),
  FOREIGN KEY (vendor_id_2) REFERENCES mdm_vendor(vendor_id),
  
  
  INDEX idx_vendor_ids (vendor_id_1, vendor_id_2),
  INDEX idx_confidence (confidence_level),
  INDEX idx_created_at (created_at)
);

CREATE INDEX idx_mdm_vendor_state ON mdm_vendor(lifecycle_state);



INSERT INTO mdm_vendor (
  source_vendor_id,
  vendor_name,
  lifecycle_state,
  match_confidence,
  source_system,
  created_by,
  approved_at,
  approved_by
) VALUES
  (1, 'Creative Mobile Technologies, LLC', 'ACTIVE', 100.00,
   'NYC_TLC_dictionary', 'initial_load',  CURRENT_TIMESTAMP, 'initial_load'),
  (2, 'Curb Mobility, LLC', 'ACTIVE', 100.00,
   'NYC_TLC_dictionary', 'initial_load',  CURRENT_TIMESTAMP, 'initial_load'),
  (6, 'Myle Technologies Inc', 'ACTIVE', 100.00,
   'NYC_TLC_dictionary', 'initial_load',  CURRENT_TIMESTAMP, 'initial_load'),
  (7, 'Helix', 'ACTIVE', 100.00,
   'NYC_TLC_dictionary', 'initial_load',  CURRENT_TIMESTAMP, 'initial_load');
   


CREATE TABLE IF NOT EXISTS mdm_golden_trip_level (

  trip_id                 BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  vendorid                INT,
  tpep_pickup_datetime    DATETIME,
  tpep_dropoff_datetime   DATETIME,
  passenger_count         DOUBLE,
  trip_distance           DOUBLE,
  ratecodeid              INT,
  store_and_fwd_flag      VARCHAR(10),
  pulocationid            INT,
  dolocationid            INT,
  payment_type            INT,
  fare_amount             DOUBLE,
  extra                   DOUBLE,
  mta_tax                 DOUBLE,
  tip_amount              DOUBLE,
  tolls_amount            DOUBLE,
  improvement_surcharge   DOUBLE,
  total_amount            DOUBLE,
  congestion_surcharge    DOUBLE,
  airport_fee             DOUBLE,
  cbd_congestion_fee      DOUBLE,
  trip_duration_minutes   DECIMAL(27,6),
  tip_pct                 DOUBLE,
  pickup_hour             INT,
  pickup_day_of_week      INT,
  pickup_day_of_month     INT,
  is_long_trip            TINYINT,
  is_short_trip           TINYINT,
  high_tip_flag           TINYINT,
  vendor_name             VARCHAR(200),
  rate_code_id            BIGINT,
  rate_code_desc          VARCHAR(200),
  pu_locationid           INT,
  pu_borough              VARCHAR(100),
  pu_zone                 VARCHAR(200),
  pu_service_zone         VARCHAR(100),
  do_locationid           INT,
  do_borough              VARCHAR(100),
  do_zone                 VARCHAR(200),
  do_service_zone         VARCHAR(100),
  data_zone               VARCHAR(50),
  data_owner              VARCHAR(100),
  pickup_year             VARCHAR(4),
  pickup_month            VARCHAR(2),
  pickup_day              VARCHAR(2),
  PRIMARY KEY (trip_id),

  INDEX idx_pickup_date   (pickup_year, pickup_month, pickup_day),
  INDEX idx_vendor        (vendorid),
  INDEX idx_pu_location   (pu_locationid),
  INDEX idx_do_location   (do_locationid),
  INDEX idx_pickup_hour   (pickup_hour),
  INDEX idx_high_tip      (high_tip_flag),
  INDEX idx_total_amount  (total_amount)
);

CREATE TABLE IF NOT EXISTS mdm_ratecode (
  ratecode_id       INT AUTO_INCREMENT PRIMARY KEY,        
  source_ratecode_id INT UNIQUE,
  rate_code_desc    VARCHAR(100) NOT NULL,
  lifecycle_state   VARCHAR(20) NOT NULL DEFAULT 'PROPOSED',
  match_confidence  DECIMAL(5,2),
  source_system     VARCHAR(50),
  created_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  created_by        VARCHAR(50) NOT NULL,
  updated_at        TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  updated_by        VARCHAR(50),
  approved_at       TIMESTAMP NULL DEFAULT NULL,
  approved_by       VARCHAR(50),
  change_id         BIGINT UNSIGNED DEFAULT NULL           
);

CREATE INDEX idx_mdm_ratecode_state ON mdm_ratecode(lifecycle_state);

CREATE TABLE mdm_ratecode_history (
  history_id        INT AUTO_INCREMENT PRIMARY KEY,
  change_id         BIGINT UNSIGNED,
  ratecode_id       INT,
  rate_code_desc    VARCHAR(100),
  lifecycle_state   VARCHAR(20),
  match_confidence  DECIMAL(5,2),
  source_system     VARCHAR(50),
  created_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  created_by        VARCHAR(50) NOT NULL,
  approved_at       TIMESTAMP NULL DEFAULT NULL,
  approved_by       VARCHAR(50),
  change_type       VARCHAR(20),
  change_reason     TEXT,
  INDEX idx_change_id (change_id),
  INDEX idx_ratecode_id (ratecode_id)
);

CREATE TABLE IF NOT EXISTS mdm_ratecode_dedup_queue (
  queue_id          INT AUTO_INCREMENT PRIMARY KEY,         
  ratecode_id_1     INT,
  ratecode_id_2     INT,
  match_score       DECIMAL(5,2),                            
  confidence_level  VARCHAR(10),
  created_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  steward_decision  VARCHAR(20),
  decision_reason   TEXT,
  decided_at        TIMESTAMP NULL DEFAULT NULL,
  decided_by        VARCHAR(50),
  
  FOREIGN KEY (ratecode_id_1) REFERENCES mdm_ratecode(ratecode_id),
  FOREIGN KEY (ratecode_id_2) REFERENCES mdm_ratecode(ratecode_id),
  
  INDEX idx_ratecode_ids (ratecode_id_1, ratecode_id_2),
  INDEX idx_confidence (confidence_level),
  INDEX idx_created_at (created_at)
);

INSERT INTO mdm_ratecode (
  source_ratecode_id,
  rate_code_desc,
  lifecycle_state,
  match_confidence,
  source_system,
  created_by,
  approved_at,
  approved_by
) VALUES
  (1,  'Standard rate',           'ACTIVE', 100.00, 'NYC_TLC_dictionary', 'initial_load', CURRENT_TIMESTAMP, 'initial_load'),
  (2,  'JFK',                     'ACTIVE', 100.00, 'NYC_TLC_dictionary', 'initial_load', CURRENT_TIMESTAMP, 'initial_load'),
  (3,  'Newark',                  'ACTIVE', 100.00, 'NYC_TLC_dictionary', 'initial_load', CURRENT_TIMESTAMP, 'initial_load'),
  (4,  'Nassau or Westchester',   'ACTIVE', 100.00, 'NYC_TLC_dictionary', 'initial_load', CURRENT_TIMESTAMP, 'initial_load'),
  (5,  'Negotiated fare',         'ACTIVE', 100.00, 'NYC_TLC_dictionary', 'initial_load', CURRENT_TIMESTAMP, 'initial_load'),
  (6,  'Group ride',              'ACTIVE', 100.00, 'NYC_TLC_dictionary', 'initial_load', CURRENT_TIMESTAMP, 'initial_load'),
  (99, 'Null/unknown',            'ACTIVE', 100.00, 'NYC_TLC_dictionary', 'initial_load', CURRENT_TIMESTAMP, 'initial_load');

CREATE TABLE IF NOT EXISTS mdm_zone (
  zone_id           INT AUTO_INCREMENT PRIMARY KEY,
  source_zone_id    INT UNIQUE,
  zone_name         VARCHAR(200) NOT NULL,
  lifecycle_state   VARCHAR(20) NOT NULL DEFAULT 'PROPOSED',
  match_confidence  DECIMAL(5,2),
  source_system     VARCHAR(50),
  created_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  created_by        VARCHAR(50) NOT NULL DEFAULT 'initial_load',
  updated_at        TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  updated_by        VARCHAR(50),
  approved_at       TIMESTAMP NULL DEFAULT NULL,
  approved_by       VARCHAR(50),
  change_id         BIGINT UNSIGNED DEFAULT NULL
);

CREATE INDEX idx_mdm_zone_state ON mdm_zone(lifecycle_state);

CREATE TABLE mdm_zone_history (
  history_id        INT AUTO_INCREMENT PRIMARY KEY,
  change_id         BIGINT UNSIGNED,
  zone_id           INT,
  zone_name         VARCHAR(200),
  lifecycle_state   VARCHAR(20),
  match_confidence  DECIMAL(5,2),
  source_system     VARCHAR(50),
  created_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  created_by        VARCHAR(50) NOT NULL,
  approved_at       TIMESTAMP NULL DEFAULT NULL,
  approved_by       VARCHAR(50),
  change_type       VARCHAR(20),
  change_reason     TEXT,
  INDEX idx_change_id (change_id),
  INDEX idx_zone_id (zone_id)
);

CREATE TABLE IF NOT EXISTS mdm_zone_dedup_queue (
  queue_id          INT AUTO_INCREMENT PRIMARY KEY,
  zone_id_1         INT,
  zone_id_2         INT,
  match_score       DECIMAL(5,2),
  confidence_level  VARCHAR(10),
  created_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  steward_decision  VARCHAR(20),
  decision_reason   TEXT,
  decided_at        TIMESTAMP NULL DEFAULT NULL,
  decided_by        VARCHAR(50),
  
  FOREIGN KEY (zone_id_1) REFERENCES mdm_zone(zone_id),
  FOREIGN KEY (zone_id_2) REFERENCES mdm_zone(zone_id),
  
  INDEX idx_zone_ids (zone_id_1, zone_id_2),
  INDEX idx_confidence (confidence_level),
  INDEX idx_created_at (created_at)
);


CREATE TABLE IF NOT EXISTS mdm_pipeline_audit (
    audit_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    
    pipeline_run_id   VARCHAR(100) NOT NULL,
    triggered_by      VARCHAR(100) NOT NULL,
    entity_type       VARCHAR(50)  NOT NULL,
    
    records_processed INT          NOT NULL DEFAULT 0,
    records_approved  INT          NOT NULL DEFAULT 0,
    
    quality_score     DECIMAL(5,2) NOT NULL DEFAULT 0.00,
    
    status            VARCHAR(30)  NOT NULL,
    
    stepfn_execution  VARCHAR(255),
    
    run_start         DATETIME     NOT NULL,
    run_end           DATETIME,
    
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_pipeline_run (pipeline_run_id),
    INDEX idx_entity_type (entity_type),
    INDEX idx_status (status),
    INDEX idx_created_at (created_at)
);

select count(*) from mdm_trip_processing_tracker where pipeline_run_id = 'mdm-run-406dc434-cc82-4a87-905a-350f054ea571';

select count(*) from mdm_pipeline_audit;


select * from mdm_vendor;
select * from mdm_ratecode;
select * from mdm_zone;
select * from mdm_golden_trip_level;

CREATE TABLE mdm_entity_match_temp (
    pipeline_run_id     BIGINT              NOT NULL,
    trip_id             BIGINT              NOT NULL,
    entity_type         VARCHAR(50)         NOT NULL,      -- e.g. 'CUSTOMER', 'DRIVER', 'VEHICLE', etc.
    source_id           VARCHAR(100)        NOT NULL,      -- source system identifier
    proposed_name       VARCHAR(255)        NULL,          -- name candidate from source
    golden_id           BIGINT              NULL,          -- matched golden/master record id
    match_confidence    DECIMAL(5,4)        NULL,          -- 0.0000 ~ 1.0000
    lifecycle_state     VARCHAR(30)         NOT NULL       DEFAULT 'NEW',
    auto_approved       BOOLEAN             NOT NULL       DEFAULT FALSE,
    dedup_queued        BOOLEAN             NOT NULL       DEFAULT FALSE,
    
    -- Recommended useful columns/indices (optional but very common in such tables)
    created_at          TIMESTAMP           NOT NULL       DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP           NULL           ON UPDATE CURRENT_TIMESTAMP,
    
    PRIMARY KEY (pipeline_run_id, trip_id, entity_type, source_id),
    INDEX idx_golden_id (golden_id),
    INDEX idx_match_confidence (match_confidence DESC),
    INDEX idx_auto_approved (auto_approved),
    INDEX idx_dedup_queued (dedup_queued)
);

ALTER TABLE mdm_golden_trip_level
ADD COLUMN vendor_golden_id INT NULL,
ADD COLUMN pu_zone_golden_id INT NULL,
ADD COLUMN do_zone_golden_id INT NULL,
ADD COLUMN ratecode_golden_id INT NULL,
ADD INDEX idx_vendor_golden (vendor_golden_id),
ADD INDEX idx_pu_zone_golden (pu_zone_golden_id),
ADD INDEX idx_do_zone_golden (do_zone_golden_id),
ADD INDEX idx_ratecode_golden (ratecode_golden_id);

select * from mdm_entity_match_temp;
select * from mdm_trip_processing_tracker;

select * from mdm_vendor_history;
select * from mdm_ratecode_history;
select * from mdm_zone_history;

select * from mdm_vendor_dedup_queue;
select * from mdm_ratecode_dedup_queue;
select * from mdm_zone_dedup_queue;

select * from mdm_pipeline_audit;



-- Approve all 3 records at once
UPDATE mdm_vendor SET lifecycle_state = 'ACTIVE', approved_at = NOW(), approved_by = 'steward_test' WHERE source_vendor_id = 989;
UPDATE mdm_ratecode SET lifecycle_state = 'ACTIVE', approved_at = NOW(), approved_by = 'steward_test' WHERE source_ratecode_id = 889;
UPDATE mdm_zone SET lifecycle_state = 'ACTIVE', approved_at = NOW(), approved_by = 'steward_test' WHERE source_zone_id IN (500, 501);

-- Verify
SELECT 'VENDOR' as type, vendor_id, vendor_name, lifecycle_state, approved_by FROM mdm_vendor WHERE source_vendor_id = 989
UNION ALL
SELECT 'RATECODE', ratecode_id, rate_code_desc, lifecycle_state, approved_by FROM mdm_ratecode WHERE source_ratecode_id = 889
UNION ALL
SELECT 'ZONE', zone_id, zone_name, lifecycle_state, approved_by FROM mdm_zone WHERE source_zone_id IN (500, 501);

