-- Silver Equipment Maintenance Table
-- Removes full duplicates while tracking history of changes

-- Step 1: Create a streaming table in silver schema
CREATE OR REFRESH STREAMING TABLE silver.equipment_maintenance_full
AS
SELECT * EXCEPT (_rescued_data)
FROM STREAM(bronze.equipment_maintenance);

-- Step 2: Create the target streaming table with SCD Type 2 columns (in silver schema)
CREATE OR REFRESH STREAMING TABLE silver.equipment_maintenance_snapshot
(
  equipment_id INT,
  site_id INT,
  equipment_type STRING,
  category STRING,
  equipment_name STRING,
  install_date DATE,
  operating_hours INT,
  last_maintenance_date DATE,
  next_maintenance_date DATE,
  health_score INT,
  status STRING,
  annual_maintenance_cost_usd DOUBLE,
  __START_AT DATE,
  __END_AT DATE,
  CONSTRAINT not_empty EXPECT (equipment_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_equipment_id EXPECT (equipment_id > 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_site EXPECT (site_id IS NOT NULL) ON VIOLATION DROP ROW
);

-- Step 3: Define CDC flow with SCD Type 2 for history tracking
-- AUTO CDC automatically deduplicates based on KEYS and SEQUENCE BY.

-- Under the hood, this is performing a MERGE INTO on the keys equipment_id and site_id
CREATE FLOW equipment_maintenance_flow AS AUTO CDC INTO silver.equipment_maintenance_snapshot
FROM STREAM(silver.equipment_maintenance_full)
KEYS (equipment_id, site_id)
SEQUENCE BY last_maintenance_date
-- If we only wanted to update a few columns
-- COLUMNS equipment_id, site_id, last_maintenance_date, health_score, status

-- This will update the _START_AT and _END_AT columns
STORED AS SCD TYPE 2;
