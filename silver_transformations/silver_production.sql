-- Step 1: Create a streaming table in silver schema (avoids duplicate with bronze.daily_production)
CREATE OR REFRESH STREAMING TABLE silver.daily_production
AS
SELECT * EXCEPT (_rescued_data)
FROM STREAM(bronze.daily_production);

