-- Mining Site Table
CREATE OR REFRESH STREAMING TABLE bronze.mine_sites
(
  CONSTRAINT not_empty EXPECT (site_id IS NOT NULL) ON VIOLATION FAIL UPDATE
)
AS SELECT *
FROM STREAM read_files(
  '${mine_site_full_path}',
  format => 'parquet',
  `cloudFiles.schemaEvolutionMode` => 'addNewColumns',
  `cloudFiles.rescuedDataColumn` => '_rescued_data',
  `includeExistingFiles` => true
);


CREATE OR REFRESH STREAMING TABLE bronze.equipment_maintenance
(
  CONSTRAINT not_empty EXPECT (equipment_id IS NOT NULL) ON VIOLATION FAIL UPDATE
)
AS SELECT *
FROM STREAM read_files(
  '${equipment_maintenance_full_path}',
  format => 'parquet',
  `cloudFiles.schemaEvolutionMode` => 'addNewColumns',
  `cloudFiles.rescuedDataColumn` => '_rescued_data',
  `includeExistingFiles` => true
);


CREATE OR REFRESH STREAMING TABLE bronze.daily_production
(
  CONSTRAINT not_empty EXPECT (site_id IS NOT NULL) ON VIOLATION FAIL UPDATE
)
AS SELECT *
FROM STREAM read_files(
  '${daily_production_full_path}',
  format => 'parquet',
  `cloudFiles.schemaEvolutionMode` => 'addNewColumns',
  `cloudFiles.rescuedDataColumn` => '_rescued_data',
  `cloudFiles.partitionColumns` => 'production_date',
  `includeExistingFiles` => true
);
