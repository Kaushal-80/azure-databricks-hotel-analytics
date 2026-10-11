# Databricks notebook source

from pyspark.sql import functions as F

# Update with your actual storage account name
storage_root = (
    "abfss://hilton@sthilton1234"
    ".dfs.core.windows.net"
)

source_files = {
    "hotels": "hotels/hotels.csv",
    "reservations": "reservations/reservations.csv",
    "guests": "guests/guests.csv",
    "rooms": "rooms/rooms.csv",
    "room_types": "room_types/room_types.csv",
    "rate_plans": "rate_plans/rate_plans.csv",
    "payments": "payments/payments.csv",
    "business_days": "business_days/business_days.csv"
}

target_schema = "hilton_catalog.bronze"

# Create each Bronze Delta table
for table_name, file_name in source_files.items():

    source_path = f"{storage_root}/raw/{file_name}"

    print(f"Processing: {file_name}")

    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("mode", "PERMISSIVE")
        .load(source_path)
    )

    # Add ingestion metadata
    df = (
        df
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_ingestion_date", F.current_date())
    )

    target_table = f"{target_schema}.{table_name}"

    # Initial portfolio-project load:
    # overwrite is convenient for repeatable testing,
    # but is not an incremental production strategy.
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

    print(f"Created {target_table}")
    print(f"Rows loaded: {spark.table(target_table).count()}")

print("Bronze ingestion completed.")


# COMMAND ----------


tables = [
    "hotels",
    "reservations",
    "guests",
    "rooms",
    "room_types",
    "rate_plans",
    "payments",
    "business_days"
]

for table in tables:
    full_name = f"hilton_catalog.bronze.{table}"
    count = spark.table(full_name).count()
    print(f"{full_name}: {count} rows")


# COMMAND ----------

display(
    spark.table("hilton_catalog.bronze.reservations")
)