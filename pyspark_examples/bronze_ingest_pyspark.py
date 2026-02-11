from pyspark.sql import SparkSession


# Global variables

# Source path for your source parquet files
source_path = "/Volumes/praju_dev/landing/mining_sample/production/*" 

# Schema location should be in your user schema or properly tagged catalog

metadata_base_path = "/Volumes/praju_dev/bronze/metadata/"

metadata_schema_location = "/Volumes/praju_dev/bronze/metadata/production/_schema"
metadata_checkpoint_location = "/Volumes/praju_dev/bronze/metadata/production/_checkpoint"


def unity_catalog_ops(spark: SparkSession, catalog: str, schema: str, target_table: str):

    # create a schema in the target catalog to store metadata
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.metadata")


    # create target table in the target schema
    spark.sql(f"CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{target_table}")


def run_stream(spark: SparkSession, 
    source_path: str, 
    metadata_schema_location: str, 
    metadata_checkpoint_location: str, 
    target_catalog: str, 
    target_schema: str, 
    target_table: str):
    
# Read stream with Auto Loader
    input_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.schemaLocation", metadata_schema_location)
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.includeExistingFiles", "true")
        .load(source_path)
    )

    # Write stream - Serverless compatible
    (
        input_df.writeStream
        .format("delta")
        .option("checkpointLocation", metadata_checkpoint_location)
        .outputMode("append")
        .trigger(availableNow=True)
        .table(f"{target_catalog}.{target_schema}.{target_table}")
    )


if __name__ == "__main__":
    
    unity_catalog_ops(spark, catalog="praju_dev", schema="bronze", target_table="production_pyspark")
    run_stream(spark, 
        source_path=source_path, 
        metadata_schema_location=metadata_schema_location, 
        metadata_checkpoint_location=metadata_checkpoint_location, 
        target_catalog="praju_dev", 
        target_schema="bronze", 
        target_table="production_pyspark")

