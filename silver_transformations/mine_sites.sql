-- Step 1: Create a streaming table in silver schema (avoids duplicate with bronze.mine_sites)
CREATE OR REFRESH STREAMING TABLE silver.mine_sites
AS
SELECT * EXCEPT (_rescued_data)
FROM STREAM(bronze.mine_sites);


