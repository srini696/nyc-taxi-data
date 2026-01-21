USE nyc_taxi_mdm;

-- Approve all 4 records
UPDATE mdm_vendor 
SET lifecycle_state = 'ACTIVE', 
    approved_at = NOW(), 
    approved_by = 'steward_test'
WHERE source_vendor_id = 888;

UPDATE mdm_ratecode 
SET lifecycle_state = 'ACTIVE', 
    approved_at = NOW(), 
    approved_by = 'steward_test'
WHERE source_ratecode_id = 888;

UPDATE mdm_zone 
SET lifecycle_state = 'ACTIVE', 
    approved_at = NOW(), 
    approved_by = 'steward_test'
WHERE source_zone_id IN (777, 778);

-- Verify approvals
SELECT 'VENDOR' AS type, vendor_id, vendor_name, lifecycle_state, approved_by 
FROM mdm_vendor WHERE source_vendor_id = 888
UNION ALL
SELECT 'RATECODE', ratecode_id, rate_code_desc, lifecycle_state, approved_by 
FROM mdm_ratecode WHERE source_ratecode_id = 888
UNION ALL
SELECT 'ZONE', zone_id, zone_name, lifecycle_state, approved_by 
FROM mdm_zone WHERE source_zone_id IN (777, 778);


SELECT * 
FROM mdm_golden_trip_level 
WHERE trip_id = (SELECT MAX(trip_id) FROM mdm_golden_trip_level);

INSERT INTO mdm_zone (source_zone_id, zone_name, lifecycle_state, match_confidence, 
    source_system, created_by, approved_at, approved_by)
VALUES
    (789, 'Manhattan Zone A', 'ACTIVE', 100.00, 'initial_load', 'admin', NOW(), 'admin'),
    (790, 'Manhattan Zone B', 'ACTIVE', 100.00, 'initial_load', 'admin', NOW(), 'admin');