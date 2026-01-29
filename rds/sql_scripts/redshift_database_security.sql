-- ============================================================================
-- REDSHIFT SERVERLESS COMPATIBLE SECURITY SETUP
-- ============================================================================
-- Error: "Operation is not supported through datashares"
-- Cause: You're using Redshift Serverless or Datashares
-- Solution: Use ROLE-based security instead of GROUP
-- ============================================================================

-- ============================================================================
-- OPTION 1: FOR REDSHIFT SERVERLESS (Recommended)
-- ============================================================================

-- Step 1: Create ROLES instead of GROUPS
CREATE ROLE analyst_role;
CREATE ROLE engineer_role;
CREATE ROLE steward_role;

-- Step 2: Create Users
CREATE USER analyst_user PASSWORD 'AnalystPass123!';
CREATE USER engineer_user PASSWORD 'EngineerPass123!';
CREATE USER steward_user PASSWORD 'StewardPass123!';
CREATE USER quicksight_user PASSWORD 'QuickSightPass123!';

-- Step 3: Grant Roles to Users
GRANT ROLE analyst_role TO analyst_user;
GRANT ROLE analyst_role TO quicksight_user;
GRANT ROLE engineer_role TO engineer_user;
GRANT ROLE steward_role TO steward_user;

-- Step 4: Grant Schema Access to Roles
GRANT USAGE ON SCHEMA nyc_taxi_dw TO ROLE analyst_role;
GRANT USAGE ON SCHEMA nyc_taxi_dw TO ROLE engineer_role;
GRANT USAGE ON SCHEMA nyc_taxi_dw TO ROLE steward_role;

-- Step 5: Grant Table Permissions

-- Analysts: Read-only access
GRANT SELECT ON ALL TABLES IN SCHEMA nyc_taxi_dw TO ROLE analyst_role;

-- Engineers: Full access
GRANT ALL ON ALL TABLES IN SCHEMA nyc_taxi_dw TO ROLE engineer_role;

-- Stewards: Read + specific write
GRANT SELECT ON ALL TABLES IN SCHEMA nyc_taxi_dw TO ROLE steward_role;
GRANT UPDATE ON TABLE nyc_taxi_dw.dim_vendor_scd2 TO ROLE steward_role;
GRANT UPDATE ON TABLE nyc_taxi_dw.dim_zone_scd2 TO ROLE steward_role;
GRANT INSERT ON TABLE nyc_taxi_dw.stg_vendor_updates TO ROLE steward_role;

-- Step 6: Set default privileges for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA nyc_taxi_dw 
GRANT SELECT ON TABLES TO ROLE analyst_role;

ALTER DEFAULT PRIVILEGES IN SCHEMA nyc_taxi_dw 
GRANT ALL ON TABLES TO ROLE engineer_role;

ALTER DEFAULT PRIVILEGES IN SCHEMA nyc_taxi_dw 
GRANT SELECT ON TABLES TO ROLE steward_role;

-- ============================================================================
-- OPTION 2: SIMPLIFIED APPROACH (If ROLE also doesn't work)
-- Just create users with direct grants
-- ============================================================================

-- Create Users
CREATE USER analyst_user PASSWORD 'AnalystPass123!';
CREATE USER engineer_user PASSWORD 'EngineerPass123!';
CREATE USER steward_user PASSWORD 'StewardPass123!';
CREATE USER quicksight_user PASSWORD 'QuickSightPass123!';

-- Grant Schema Usage
GRANT USAGE ON SCHEMA nyc_taxi_dw TO analyst_user;
GRANT USAGE ON SCHEMA nyc_taxi_dw TO engineer_user;
GRANT USAGE ON SCHEMA nyc_taxi_dw TO steward_user;
GRANT USAGE ON SCHEMA nyc_taxi_dw TO quicksight_user;

-- Grant Permissions Directly to Users

-- Analyst (Read-Only)
GRANT SELECT ON ALL TABLES IN SCHEMA nyc_taxi_dw TO analyst_user;
GRANT SELECT ON ALL TABLES IN SCHEMA nyc_taxi_dw TO quicksight_user;

-- Engineer (Full Access)
GRANT ALL ON SCHEMA nyc_taxi_dw TO engineer_user;
GRANT ALL ON ALL TABLES IN SCHEMA nyc_taxi_dw TO engineer_user;

-- Steward (Read + Limited Write)
GRANT SELECT ON ALL TABLES IN SCHEMA nyc_taxi_dw TO steward_user;
GRANT UPDATE ON TABLE nyc_taxi_dw.dim_vendor_scd2 TO steward_user;
GRANT UPDATE ON TABLE nyc_taxi_dw.dim_zone_scd2 TO steward_user;

-- ============================================================================
-- VERIFY PERMISSIONS
-- ============================================================================

-- Check users created
SELECT usename, usecreatedb, usesuper 
FROM pg_user 
ORDER BY usename;

-- Check roles (if using roles)
SELECT role_name 
FROM svv_roles 
ORDER BY role_name;

-- Check user permissions
SELECT 
    schemaname,
    tablename,
    usename,
    has_select,
    has_insert,
    has_update,
    has_delete
FROM 
    (SELECT 
        n.nspname AS schemaname,
        c.relname AS tablename,
        u.usename,
        has_table_privilege(u.usename, c.oid, 'SELECT') AS has_select,
        has_table_privilege(u.usename, c.oid, 'INSERT') AS has_insert,
        has_table_privilege(u.usename, c.oid, 'UPDATE') AS has_update,
        has_table_privilege(u.usename, c.oid, 'DELETE') AS has_delete
    FROM pg_class c
    JOIN pg_namespace n ON c.relnamespace = n.oid
    CROSS JOIN pg_user u
    WHERE n.nspname = 'nyc_taxi_dw'
    AND c.relkind = 'r'
    AND u.usename IN ('analyst_user', 'engineer_user', 'steward_user', 'quicksight_user'))
WHERE has_select OR has_insert OR has_update OR has_delete;

-- ============================================================================
-- CREATE MASKED VIEW FOR ANALYSTS
-- ============================================================================

CREATE OR REPLACE VIEW nyc_taxi_dw.v_trip_masked AS
SELECT 
    trip_sk,
    pickup_date_sk,
    pickup_time_sk,
    vendor_sk,
    pickup_zone_sk,
    dropoff_zone_sk,
    -- Mask exact amounts - show rounded values
    ROUND(fare_amount / 10) * 10 AS fare_amount_rounded,
    ROUND(tip_amount / 5) * 5 AS tip_amount_rounded,
    ROUND(total_amount / 10) * 10 AS total_amount_rounded,
    -- Non-sensitive fields shown as-is
    trip_distance,
    passenger_count,
    trip_duration_minutes,
    is_rush_hour,
    is_airport_trip,
    is_cash_trip
FROM nyc_taxi_dw.fact_trip_scd2;

-- Grant analysts access only to masked view
GRANT SELECT ON nyc_taxi_dw.v_trip_masked TO analyst_user;
GRANT SELECT ON nyc_taxi_dw.v_trip_masked TO quicksight_user;

-- Revoke direct fact table access from analysts (if previously granted)
-- REVOKE SELECT ON nyc_taxi_dw.fact_trip_scd2 FROM analyst_user;
-- REVOKE SELECT ON nyc_taxi_dw.fact_trip_scd2 FROM quicksight_user;

-- ============================================================================
-- TEST ACCESS (Run as each user to verify)
-- ============================================================================

-- Test as analyst_user
-- SET SESSION AUTHORIZATION analyst_user;
-- SELECT * FROM nyc_taxi_dw.v_trip_masked LIMIT 5;  -- Should work
-- SELECT * FROM nyc_taxi_dw.fact_trip_scd2 LIMIT 5; -- Should fail (if revoked)
-- RESET SESSION AUTHORIZATION;

-- Test as engineer_user  
-- SET SESSION AUTHORIZATION engineer_user;
-- SELECT * FROM nyc_taxi_dw.fact_trip_scd2 LIMIT 5; -- Should work
-- DELETE FROM nyc_taxi_dw.fact_trip_scd2 WHERE 1=0; -- Should work (no rows affected)
-- RESET SESSION AUTHORIZATION;

-- ============================================================================
-- IF YOU'RE USING REDSHIFT SERVERLESS VIA QUERY EDITOR V2
-- You may need to connect to the actual database, not a datashare
-- ============================================================================

/*
TROUBLESHOOTING:

1. Make sure you're connected to the actual database, not a datashare
   - In Query Editor v2, check the connection dropdown
   - Select your workgroup directly, not a datashare

2. If using datashares, user management must be done on the PRODUCER cluster
   - Datashare consumers cannot create users/groups/roles

3. For Redshift Serverless:
   - Connect using the workgroup endpoint
   - Use "admin" or your superuser credentials
   - Then create users/roles

4. Check your connection:
   SELECT current_database(), current_user, current_schema();
   
   If it shows a datashare database, you need to switch to the actual database.
*/

-- ============================================================================
-- REDSHIFT SERVERLESS: CONNECT CORRECTLY
-- ============================================================================

/*
In Query Editor v2:

1. Click the database dropdown (top left)
2. Click "Add connection" or select existing
3. Choose "Serverless" 
4. Select your workgroup: default-workgroup (or your workgroup name)
5. Database name: dev (or your database)
6. Use "Federated user" or "Database credentials"
7. Click "Connect"

Then run the user creation commands.
*/
