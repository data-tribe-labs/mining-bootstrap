from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random
from pyspark.sql.functions import col, expr, rand, when, date_add, lit, hour, floor, to_timestamp, make_interval
from pyspark.sql.types import *
from datetime import datetime, timedelta
import argparse


def create_unity_catalog_objects(spark: SparkSession, catalog: str, schema: str, volume: str):
    # Create Unity Catalog objects
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")

    print(f"✓ Unity Catalog objects created: {catalog}.{schema}.{volume}")


def generate_mine_sites_table(spark: SparkSession, mine_site_full_path: str):
# Create Mine Sites table - 50 sites across different regions
    mine_sites_data = [
        (1, "Copper Ridge Mine", "Arizona, USA", "Copper", "Open Pit", datetime(2015, 3, 15), "Active", 450),
        (2, "Silver Valley Mine", "Nevada, USA", "Silver", "Underground", datetime(2010, 7, 22), "Active", 280),
        (3, "Iron Mountain Mine", "Minnesota, USA", "Iron Ore", "Open Pit", datetime(2008, 1, 10), "Active", 620),
        (4, "Gold Creek Mine", "Alaska, USA", "Gold", "Underground", datetime(2018, 9, 5), "Active", 180),
        (5, "Coal Basin Mine", "Wyoming, USA", "Coal", "Surface", datetime(2012, 11, 30), "Active", 520),
        (6, "Platinum Peak Mine", "Montana, USA", "Platinum", "Underground", datetime(2016, 4, 12), "Active", 320),
        (7, "Zinc Valley Mine", "Idaho, USA", "Zinc", "Open Pit", datetime(2014, 8, 20), "Active", 380),
        (8, "Nickel Ridge Mine", "Ontario, Canada", "Nickel", "Underground", datetime(2011, 2, 5), "Active", 410),
        (9, "Bauxite Hills Mine", "Queensland, Australia", "Bauxite", "Surface", datetime(2013, 6, 18), "Active", 550),
        (10, "Diamond Point Mine", "Northwest Territories, Canada", "Diamond", "Underground", datetime(2017, 10, 8), "Active", 220),
        (11, "Lithium Springs Mine", "Nevada, USA", "Lithium", "Open Pit", datetime(2019, 1, 15), "Active", 290),
        (12, "Cobalt Creek Mine", "Idaho, USA", "Cobalt", "Underground", datetime(2020, 3, 22), "Active", 195),
        (13, "Rare Earth Valley", "California, USA", "Rare Earth", "Open Pit", datetime(2018, 7, 10), "Active", 340),
        (14, "Tungsten Ridge Mine", "Colorado, USA", "Tungsten", "Underground", datetime(2015, 11, 5), "Active", 265),
        (15, "Manganese Basin", "South Africa", "Manganese", "Surface", datetime(2012, 9, 14), "Active", 480),
        (16, "Chromite Mountain", "Kazakhstan", "Chromite", "Open Pit", datetime(2014, 5, 28), "Active", 425),
        (17, "Phosphate Valley", "Morocco", "Phosphate", "Surface", datetime(2010, 12, 3), "Active", 590),
        (18, "Potash Plains Mine", "Saskatchewan, Canada", "Potash", "Underground", datetime(2016, 8, 17), "Active", 370),
        (19, "Uranium Ridge Mine", "Namibia", "Uranium", "Open Pit", datetime(2013, 4, 9), "Maintenance", 310),
        (20, "Graphite Creek Mine", "Brazil", "Graphite", "Surface", datetime(2017, 2, 21), "Active", 440),
        # Additional 30 sites to reach 50 total
        (21, "Emerald Valley Mine", "Colombia", "Emerald", "Underground", datetime(2016, 5, 12), "Active", 210),
        (22, "Tin Mountain Mine", "Bolivia", "Tin", "Open Pit", datetime(2011, 8, 19), "Active", 390),
        (23, "Lead Ridge Mine", "Missouri, USA", "Lead", "Underground", datetime(2009, 3, 7), "Active", 330),
        (24, "Molybdenum Peak", "Chile", "Molybdenum", "Open Pit", datetime(2014, 11, 22), "Active", 510),
        (25, "Titanium Sands Mine", "Western Australia", "Titanium", "Surface", datetime(2015, 6, 30), "Active", 470),
        (26, "Vanadium Valley", "Russia", "Vanadium", "Open Pit", datetime(2012, 2, 14), "Active", 360),
        (27, "Antimony Creek Mine", "China", "Antimony", "Underground", datetime(2010, 9, 8), "Active", 280),
        (28, "Beryllium Ridge", "Utah, USA", "Beryllium", "Open Pit", datetime(2017, 4, 25), "Active", 240),
        (29, "Bismuth Basin", "Peru", "Bismuth", "Underground", datetime(2013, 12, 11), "Active", 200),
        (30, "Cadmium Valley Mine", "Mexico", "Cadmium", "Surface", datetime(2016, 7, 18), "Active", 310),
        (31, "Selenium Springs", "Japan", "Selenium", "Underground", datetime(2011, 1, 29), "Active", 190),
        (32, "Tellurium Peak", "Canada", "Tellurium", "Open Pit", datetime(2015, 10, 6), "Active", 270),
        (33, "Indium Ridge Mine", "South Korea", "Indium", "Underground", datetime(2018, 3, 14), "Active", 220),
        (34, "Gallium Creek", "Germany", "Gallium", "Surface", datetime(2014, 8, 21), "Active", 250),
        (35, "Germanium Valley", "Belgium", "Germanium", "Underground", datetime(2012, 5, 9), "Active", 230),
        (36, "Copper Canyon II", "New Mexico, USA", "Copper", "Open Pit", datetime(2019, 2, 17), "Active", 490),
        (37, "Silver Peak II", "British Columbia, Canada", "Silver", "Underground", datetime(2016, 11, 3), "Active", 300),
        (38, "Iron Hills II", "Sweden", "Iron Ore", "Open Pit", datetime(2010, 6, 28), "Active", 580),
        (39, "Gold Rush Mine", "Nevada, USA", "Gold", "Underground", datetime(2017, 9, 15), "Active", 210),
        (40, "Coal Valley II", "West Virginia, USA", "Coal", "Surface", datetime(2011, 4, 12), "Active", 540),
        (41, "Platinum Ridge II", "Zimbabwe", "Platinum", "Underground", datetime(2015, 12, 8), "Active", 340),
        (42, "Zinc Mountain II", "Ireland", "Zinc", "Open Pit", datetime(2013, 7, 24), "Active", 370),
        (43, "Nickel Basin II", "New Caledonia", "Nickel", "Surface", datetime(2016, 1, 19), "Active", 420),
        (44, "Bauxite Ridge II", "Guinea", "Bauxite", "Open Pit", datetime(2014, 10, 5), "Active", 560),
        (45, "Diamond Creek II", "Botswana", "Diamond", "Underground", datetime(2018, 5, 22), "Active", 240),
        (46, "Lithium Lake Mine", "Argentina", "Lithium", "Surface", datetime(2019, 8, 11), "Active", 310),
        (47, "Cobalt Ridge II", "Democratic Republic of Congo", "Cobalt", "Open Pit", datetime(2015, 3, 27), "Active", 380),
        (48, "Rare Earth Peak II", "Vietnam", "Rare Earth", "Underground", datetime(2017, 11, 14), "Active", 290),
        (49, "Tungsten Valley II", "Portugal", "Tungsten", "Open Pit", datetime(2012, 6, 6), "Active", 260),
        (50, "Manganese Ridge II", "Gabon", "Manganese", "Surface", datetime(2016, 9, 30), "Maintenance", 450)
    ]

    mine_sites_schema = StructType([
        StructField("site_id", IntegerType(), False),
        StructField("site_name", StringType(), False),
        StructField("location", StringType(), False),
        StructField("primary_mineral", StringType(), False),
        StructField("mining_method", StringType(), False),
        StructField("operational_since", DateType(), False),
        StructField("status", StringType(), False),
        StructField("workforce_count", IntegerType(), False)
    ])

    df_mine_sites = spark.createDataFrame(mine_sites_data, mine_sites_schema)

    # Save as Parquet files
    df_mine_sites.write.mode("overwrite").parquet(mine_site_full_path)

    print(f"✓ Mine Sites data written to Parquet: {mine_site_full_path}")
    print(f"  Total sites: {df_mine_sites.count()}")



def generate_production_data(spark: SparkSession, daily_production_full_path: str):
    # Configuration for data generation
    # change to 5 or 10 if you want it to complete faster.
    days_of_history = 365 * 20  # 20 years
    start_date = datetime(2005, 1, 1)
    hours_per_day = 24
    num_sites = 50

    # Calculate total records
    total_hours = days_of_history * hours_per_day
    total_records = total_hours * num_sites

    print(f"Generating production data using Spark distributed processing...")
    print(f"Configuration:")
    print(f"  - Time range: {days_of_history} days ({days_of_history/365:.1f} years)")
    print(f"  - Sites: {num_sites}")
    print(f"  - Granularity: Hourly")
    print(f"  - Expected records: ~{total_records:,}")
    print(f"\nUsing Spark to distribute data generation across cluster workers...")

    # Production parameters as a lookup table
    production_params_data = [
        (1, 1200, 200, 0.8, 1.2), (2, 800, 150, 2.5, 4.0), (3, 2500, 400, 55.0, 65.0),
        (4, 150, 30, 8.0, 15.0), (5, 3000, 500, 70.0, 80.0), (6, 600, 100, 3.5, 6.0),
        (7, 1800, 300, 4.0, 7.5), (8, 2200, 350, 1.2, 2.8), (9, 4500, 600, 45.0, 55.0),
        (10, 100, 20, 15.0, 25.0), (11, 900, 150, 0.5, 1.5), (12, 500, 80, 2.0, 4.5),
        (13, 700, 120, 5.0, 12.0), (14, 450, 70, 1.5, 3.5), (15, 3500, 500, 35.0, 48.0),
        (16, 2800, 400, 38.0, 52.0), (17, 5000, 700, 28.0, 35.0), (18, 2000, 300, 22.0, 32.0),
        (19, 800, 150, 0.08, 0.15), (20, 1500, 250, 8.0, 15.0), (21, 120, 25, 12.0, 20.0),
        (22, 1600, 280, 3.5, 6.5), (23, 1400, 240, 4.5, 8.0), (24, 2100, 350, 0.3, 0.8),
        (25, 3800, 550, 42.0, 58.0), (26, 1900, 320, 1.8, 3.2), (27, 950, 160, 5.5, 9.5),
        (28, 380, 65, 8.0, 14.0), (29, 420, 70, 6.0, 11.0), (30, 1100, 190, 2.2, 4.8),
        (31, 340, 55, 9.0, 16.0), (32, 480, 80, 7.0, 13.0), (33, 520, 90, 5.0, 10.0),
        (34, 610, 105, 4.5, 8.5), (35, 560, 95, 6.5, 11.5), (36, 1350, 230, 0.9, 1.4),
        (37, 850, 145, 2.8, 4.5), (38, 2700, 450, 52.0, 68.0), (39, 165, 35, 9.0, 17.0),
        (40, 3200, 530, 68.0, 82.0), (41, 640, 110, 3.8, 6.5), (42, 1750, 295, 4.2, 7.8),
        (43, 2350, 390, 1.4, 3.0), (44, 4700, 620, 43.0, 57.0), (45, 110, 22, 16.0, 28.0),
        (46, 980, 165, 0.6, 1.8), (47, 530, 88, 2.3, 5.0), (48, 730, 125, 6.0, 13.0),
        (49, 470, 75, 1.7, 3.8), (50, 3600, 510, 33.0, 50.0)
    ]

    params_schema = StructType([
        StructField("site_id", IntegerType(), False),
        StructField("base_tons", IntegerType(), False),
        StructField("variance", IntegerType(), False),
        StructField("grade_min", DoubleType(), False),
        StructField("grade_max", DoubleType(), False)
    ])

    df_params = spark.createDataFrame(production_params_data, params_schema)

    print(f"Step 1: Creating base time series...")

    # Step 1: Create base DataFrame with all hour records using Spark range
    df_base = spark.range(0, total_hours) \
        .withColumn("hour_offset", col("id")) \
        .withColumn("day_offset", floor(col("hour_offset") / hours_per_day)) \
        .withColumn("hour_of_day", col("hour_offset") % hours_per_day) \
        .withColumn("production_datetime", 
                    date_add(lit(start_date), col("day_offset").cast("int")).cast("timestamp") + make_interval(lit(0), lit(0), lit(0), lit(0), col("hour_of_day").cast("int"), lit(0), lit(0))) \
        .withColumn("production_date", col("production_datetime").cast("date")) \
        .drop("id")

    print(f"Step 2: Cross-joining with {num_sites} sites...")

    # Step 2: Cross join with sites to create all site-hour combinations
    df_sites = spark.range(1, num_sites + 1).withColumnRenamed("id", "site_id")
    df_cross = df_base.crossJoin(df_sites)

    print(f"Step 3: Joining with production parameters...")

    # Step 3: Join with production parameters
    df_with_params = df_cross.join(df_params, "site_id")

    print(f"Step 4: Generating production data with random variations...")

    # Step 4: Generate production data using Spark SQL expressions
    df_production = df_with_params \
        .withColumn("random_seed", rand()) \
        .withColumn("is_downtime", when(col("random_seed") < 0.05, lit(True)).otherwise(lit(False))) \
        .filter(~col("is_downtime")) \
        .withColumn("seasonal_factor", lit(1.0) + (rand() * 0.3 - 0.15)) \
        .withColumn("tons_extracted", 
                    ((col("base_tons") / hours_per_day + (rand() * col("variance") * 2 - col("variance")) / hours_per_day) * col("seasonal_factor")).cast("int")) \
        .withColumn("mineral_grade_percent", 
                    col("grade_min") + rand() * (col("grade_max") - col("grade_min"))) \
        .withColumn("operating_hours", lit(1.0)) \
        .withColumn("cost_per_ton_usd", lit(25.0) + rand() * 30.0) \
        .withColumn("total_cost_usd", col("tons_extracted") * col("cost_per_ton_usd")) \
        .select(
            "site_id",
            "production_datetime",
            "production_date",
            "tons_extracted",
            "mineral_grade_percent",
            "operating_hours",
            "cost_per_ton_usd",
            "total_cost_usd"
        )

    # Step 5: Write to Parquet with date partitioning (730 partitions instead of 175K)
    print(f"\nWriting to Parquet (partitioned by date for better performance)...")

    df_production.write.mode("overwrite") \
        .partitionBy("production_date") \
        .parquet(daily_production_full_path)

    print(f"\n✓ Daily Production data written to Parquet: {daily_production_full_path}")
    print(f"  Estimated records: ~{total_records:,}")
    print(f"  Date range: {start_date.date()} to {(start_date + timedelta(days=days_of_history-1)).date()}")


# Create Equipment Maintenance tracking data
# TO SCALE: Increase equipment per site, add maintenance history records, sensor readings
def generate_equipment_maintenance_data(spark: SparkSession, equipment_maintenance_full_path: str):
    equipment_data = []
    equipment_types = [
        ("Excavator", "Heavy", 5000, 12000),
        ("Haul Truck", "Heavy", 8000, 15000),
        ("Drill Rig", "Heavy", 3000, 10000),
        ("Loader", "Medium", 4000, 9000),
        ("Crusher", "Stationary", 2000, 8000),
        ("Conveyor", "Stationary", 1500, 7000),
        ("Grader", "Medium", 3500, 8500),
        ("Dozer", "Heavy", 4500, 11000),
        ("Water Truck", "Support", 2500, 6000),
        ("Generator", "Stationary", 1000, 5000)
    ]

    equipment_id = 1
    for site_id in range(1, 51):  # 50 sites
        # Each site has 8-12 pieces of equipment
        num_equipment = random.randint(8, 12)
        
        for _ in range(num_equipment):
            equip_type, category, base_hours, max_hours = random.choice(equipment_types)
            
            # Generate equipment history
            install_year = random.randint(2015, 2023)
            install_date = datetime(install_year, random.randint(1, 12), random.randint(1, 28))
            
            # Operating hours based on age
            years_in_service = 2026 - install_year
            operating_hours = base_hours + random.randint(0, years_in_service * 2000)
            
            # Last maintenance within last 60 days
            last_maintenance = datetime(2026, 1, 1) + timedelta(days=random.randint(-60, 0))
            
            # Next maintenance 30-120 days out
            next_maintenance = datetime(2026, 2, 6) + timedelta(days=random.randint(30, 120))
            
            # Health score (0-100) - degrades with operating hours
            base_health = 95 - (operating_hours / max_hours) * 30
            health_score = max(60, min(98, int(base_health + random.randint(-10, 10))))
            
            if health_score < 70:
                status = "Needs Attention"
            elif health_score < 80:
                status = "Monitor"
            else:
                status = "Operational"
            
            # Maintenance cost increases with age and usage
            annual_maintenance_cost = round(5000 + (operating_hours / 100) * random.uniform(0.8, 1.2), 2)
            
            equipment_data.append((
                equipment_id,
                site_id,
                equip_type,
                category,
                f"{equip_type}-{equipment_id:04d}",
                install_date,
                operating_hours,
                last_maintenance,
                next_maintenance,
                health_score,
                status,
                annual_maintenance_cost
            ))
            
            equipment_id += 1

    equipment_schema = StructType([
        StructField("equipment_id", IntegerType(), False),
        StructField("site_id", IntegerType(), False),
        StructField("equipment_type", StringType(), False),
        StructField("category", StringType(), False),
        StructField("equipment_name", StringType(), False),
        StructField("install_date", DateType(), False),
        StructField("operating_hours", IntegerType(), False),
        StructField("last_maintenance_date", DateType(), False),
        StructField("next_maintenance_date", DateType(), False),
        StructField("health_score", IntegerType(), False),
        StructField("status", StringType(), False),
        StructField("annual_maintenance_cost_usd", DoubleType(), False)
    ])

    df_equipment = spark.createDataFrame(equipment_data, equipment_schema)

    # Save as Parquet files
    df_equipment.write.mode("overwrite").parquet(equipment_maintenance_full_path)

    print(f"✓ Equipment Maintenance data written to Parquet: {equipment_maintenance_full_path}")
    print(f"  Total equipment: {df_equipment.count():,}")
    print(f"  Average equipment per site: {df_equipment.count() / 50:.1f}")


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Setup base datasets for mining bootstrap"
    )
    parser.add_argument(
        "catalog",
        help="Target catalog name (e.g., praju_dev)"
    )
    parser.add_argument(
        "landing_data_schema",
        help="Landing schema name (e.g., landing)"
    )
    parser.add_argument(
        "landing_data_volume",
        help="Volume name (e.g., mining_sample)"
    )
    
    args = parser.parse_args()
 
    # Use the parameters
    print(f"Catalog: {args.catalog}")
    print(f"Schema: {args.landing_data_schema}")
    print(f"Volume: {args.landing_data_volume}")

    return args.catalog, args.landing_data_schema, args.landing_data_volume    

if __name__ == "__main__":

    catalog, landing_data_schema, landing_data_volume = parse_arguments()

    # Base path to store the data
    full_volume_path = f"/Volumes/{catalog}/{landing_data_schema}/{landing_data_volume}"

    # Generate parquet data save paths
    mine_site_full_path = f"{full_volume_path}/mine_sites"
    daily_production_full_path = f"{full_volume_path}/production"
    equipment_maintenance_full_path = f"{full_volume_path}/equipment_maintenance"

    # Create catalog, schema and volumes if they don't exist
    create_unity_catalog_objects(spark, catalog, landing_data_schema, landing_data_volume)

    # Generate data
    generate_mine_sites_table(spark, mine_site_full_path)
    generate_production_data(spark, daily_production_full_path)
    generate_equipment_maintenance_data(spark, equipment_maintenance_full_path)


