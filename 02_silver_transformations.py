# Databricks notebook source
reservations = spark.table(
    "hilton_catalog.bronze.reservations"
)

reservations.printSchema()
display(reservations.limit(10))

# COMMAND ----------


from pyspark.sql import functions as F

source = spark.table("hilton_catalog.bronze.reservations")

# Standardize key text fields and date columns
df = (
    source
    .withColumn("reservation_id", F.trim("reservation_id"))
    .withColumn("hotel_id", F.trim("hotel_id"))
    .withColumn("guest_id", F.trim("guest_id"))
    .withColumn("status", F.upper(F.trim("status")))
    .withColumn("payment_status", F.upper(F.trim("payment_status")))
    .withColumn("check_in_date", F.to_date("check_in_date"))
    .withColumn("check_out_date", F.to_date("check_out_date"))
    .withColumn("booking_date", F.to_date("booking_date"))
    .withColumn("adults", F.col("adults").cast("int"))
    .withColumn("children", F.col("children").cast("int"))
    .withColumn("rooms", F.col("rooms").cast("int"))
    .withColumn("total_amount", F.col("total_amount").cast("decimal(12,2)"))
)

# Fill only fields where a default is appropriate
df = (
    df
    .withColumn("adults", F.coalesce("adults", F.lit(1)))
    .withColumn("children", F.coalesce("children", F.lit(0)))
    .withColumn("rooms", F.coalesce("rooms", F.lit(1)))
    .withColumn(
        "length_of_stay",
        F.datediff("check_out_date", "check_in_date")
    )
    .withColumn(
        "booking_lead_days",
        F.datediff("check_in_date", "booking_date")
    )
)

# Flag invalid business records
invalid_condition = (
    F.col("reservation_id").isNull()
    | (F.col("reservation_id") == "")
    | F.col("hotel_id").isNull()
    | F.col("check_in_date").isNull()
    | F.col("check_out_date").isNull()
    | (F.col("check_out_date") <= F.col("check_in_date"))
    | F.col("rooms").isNull()
    | (F.col("rooms") <= 0)
    | F.col("total_amount").isNull()
    | (F.col("total_amount") < 0)
)

invalid = df.filter(invalid_condition)
valid = df.filter(~invalid_condition)

# Remove repeated reservation keys from the valid dataset.
# For conflicting duplicate keys, investigate before choosing a record.
valid = valid.dropDuplicates(["reservation_id"])

# Save invalid records for investigation
(
    invalid
    .withColumn("_quarantined_at", F.current_timestamp())
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hilton_catalog.silver.reservation_quarantine")
)

# Save the cleaned reservation table
(
    valid
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hilton_catalog.silver.reservations")
)

print("Valid reservations:", valid.count())
print("Quarantined records:", invalid.count())


# COMMAND ----------


business_days = spark.table(
    "hilton_catalog.bronze.business_days"
)

silver_business_days = (
    business_days
    .withColumn("business_date", F.to_date("business_date"))
    .withColumn("rooms_sold", F.col("rooms_sold").cast("int"))
    .withColumn("rooms_available", F.col("rooms_available").cast("int"))
    .dropDuplicates(["business_day_id"])
    .withColumn(
        "occupancy_pct",
        F.round(
            F.col("rooms_sold") * 100.0
            / F.when(
                F.col("rooms_available") > 0,
                F.col("rooms_available")
            ),
            2
        )
    )
)

(
    silver_business_days
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hilton_catalog.silver.business_days")
)

display(silver_business_days)


# COMMAND ----------


from pyspark.sql import functions as F

simple_tables = [
    "hotels",
    "guests",
    "rooms",
    "room_types",
    "rate_plans",
    "payments"
]

def clean_simple_table(table_name):
    df = spark.table(f"hilton_catalog.bronze.{table_name}")

    # Trim strings without changing numeric columns
    for field in df.schema.fields:
        if field.dataType.simpleString() == "string":
            df = df.withColumn(
                field.name,
                F.trim(F.col(field.name))
            )

    # Remove fully identical rows
    df = df.dropDuplicates()

    # Convert payment date explicitly
    if table_name == "payments":
        df = df.withColumn(
            "payment_date",
            F.to_date("payment_date")
        )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"hilton_catalog.silver.{table_name}")
    )

    print(f"{table_name}: {df.count()} rows written")

for table_name in simple_tables:
    clean_simple_table(table_name)


# COMMAND ----------

silver_tables = [
    "hotels",
    "guests",
    "rooms",
    "room_types",
    "rate_plans",
    "payments",
    "business_days",
    "reservations",
    "reservation_quarantine"
]

for table in silver_tables:
    full_name = f"hilton_catalog.silver.{table}"
    print(f"{table}: {spark.table(full_name).count()} rows")

# COMMAND ----------


from pyspark.sql import functions as F

bronze_schema = "hilton_catalog.bronze"

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

for table_name in tables:
    full_table_name = f"{bronze_schema}.{table_name}"

    print(f"\n{'=' * 60}")
    print(f"Table: {full_table_name}")

    df = spark.table(full_table_name)

    print(f"Row count: {df.count()}")
    df.printSchema()


# COMMAND ----------


from pyspark.sql import functions as F

bronze = "hilton_catalog.bronze"
silver = "hilton_catalog.silver"

# Read the Bronze reservations table
reservations = spark.table(f"{bronze}.reservations")

# Standardize text fields and validate business rules
reservations_clean = (
    reservations
    .withColumn("reservation_id", F.trim("reservation_id"))
    .withColumn("hotel_id", F.trim("hotel_id"))
    .withColumn("guest_id", F.trim("guest_id"))
    .withColumn("status", F.upper(F.trim("status")))
    .withColumn("payment_status", F.upper(F.trim("payment_status")))
    .withColumn(
        "invalid_reason",
        F.when(
            F.col("reservation_id").isNull() |
            (F.col("reservation_id") == ""),
            "Missing reservation_id"
        )
        .when(
            F.col("hotel_id").isNull() | (F.col("hotel_id") == ""),
            "Missing hotel_id"
        )
        .when(
            F.col("check_in_date").isNull() |
            F.col("check_out_date").isNull(),
            "Missing check-in or check-out date"
        )
        .when(
            F.col("check_out_date") <= F.col("check_in_date"),
            "Check-out must be after check-in"
        )
        .when(
            F.col("booking_date").isNotNull() &
            (F.col("booking_date") > F.col("check_in_date")),
            "Booking date is after check-in"
        )
        .when(
            (F.col("adults") < 0) |
            (F.col("children") < 0) |
            (F.col("rooms") <= 0),
            "Invalid guest or room count"
)
        .when(
            F.col("total_amount") < 0,
            "Negative reservation amount"
        )
    )
)

# Separate invalid records from valid records
reservation_quarantine = reservations_clean.filter(
    F.col("invalid_reason").isNotNull()
)

reservation_valid = reservations_clean.filter(
    F.col("invalid_reason").isNull()
)

# Remove the validation helper from the curated output
reservation_valid = reservation_valid.drop("invalid_reason")

# Deduplicate only exact reservation IDs after validation.
# Keep one record per reservation ID for this initial project load.
reservation_valid = reservation_valid.dropDuplicates(["reservation_id"])

# Write Silver tables
(
    reservation_valid.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{silver}.reservations")
)

(
    reservation_quarantine.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{silver}.reservation_quarantine")
)

print("Valid reservation rows:", spark.table(f"{silver}.reservations").count())
print("Quarantined reservation rows:", spark.table(f"{silver}.reservation_quarantine").count())


# COMMAND ----------


display(
    spark.table("hilton_catalog.silver.reservations")
    .select(
        "reservation_id",
        "hotel_id",
        "guest_id",
        "check_in_date",
        "check_out_date",
        "status",
        "rooms",
        "total_amount"
    )
    .limit(10)
)

display(
    spark.table("hilton_catalog.silver.reservation_quarantine")
    .groupBy("invalid_reason")
    .count()
    .orderBy(F.desc("count"))
)


# COMMAND ----------


from pyspark.sql import functions as F

reservations = spark.table("hilton_catalog.silver.reservations")

print("Total Silver reservations:", reservations.count())

print(
    "Duplicate reservation IDs:",
    reservations.groupBy("reservation_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(
    "Invalid date ranges:",
    reservations.filter(
        F.col("check_out_date") <= F.col("check_in_date")
    ).count()
)

print(
    "Negative amounts:",
    reservations.filter(F.col("total_amount") < 0).count()
)


# COMMAND ----------


from pyspark.sql import functions as F

bronze = "hilton_catalog.bronze"
silver = "hilton_catalog.silver"

# Tables with their primary identifiers
table_keys = {
    "hotels": ["hotel_id"],
    "guests": ["guest_id"],
    "rooms": ["room_id"],
    "room_types": ["room_type_id"],
    "rate_plans": ["rate_plan_id"],
    "payments": ["payment_id"]
}

for table_name, keys in table_keys.items():
    print(f"\nProcessing {table_name}...")

    df = spark.table(f"{bronze}.{table_name}")

    # Standardize string columns without changing dates or numeric types
    for column_name, data_type in df.dtypes:
        if data_type == "string" and not column_name.startswith("_"):
            df = df.withColumn(
                column_name,
                F.when(
                    F.trim(F.col(column_name)) == "",
                    None
                ).otherwise(F.trim(F.col(column_name)))
            )

    # Normalize common status fields
    if "status" in df.columns:
        df = df.withColumn("status", F.upper(F.col("status")))

    if "payment_status" in df.columns:
        df = df.withColumn(
            "payment_status",
            F.upper(F.col("payment_status"))
        )

    # Separate rows without a primary key
    missing_key_condition = None

    for key in keys:
        condition = F.col(key).isNull() | (F.col(key) == "")
        missing_key_condition = (
            condition if missing_key_condition is None
            else missing_key_condition | condition
        )

    quarantine = df.filter(missing_key_condition)
    valid = df.filter(~missing_key_condition)

    # Remove duplicate primary keys for this initial load
    valid = valid.dropDuplicates(keys)

    # Write cleaned and quarantined data
    (
        valid.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{silver}.{table_name}")
    )

    (
        quarantine.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{silver}.{table_name}_quarantine")
    )

    print("Valid rows:", valid.count())
    print("Rows missing key:", quarantine.count())

print("\nSix additional Silver tables processed.")


# COMMAND ----------


from pyspark.sql import functions as F

bronze = "hilton_catalog.bronze"
silver = "hilton_catalog.silver"

business_days = spark.table(f"{bronze}.business_days")

# Standardize identifiers
business_days = (
    business_days
    .withColumn("business_day_id", F.trim("business_day_id"))
    .withColumn("hotel_id", F.trim("hotel_id"))
)

# Validate records
business_days_checked = business_days.withColumn(
    "invalid_reason",
    F.when(
        F.col("business_day_id").isNull() |
        (F.col("business_day_id") == ""),
        "Missing business_day_id"
    )
    .when(
        F.col("hotel_id").isNull() |
        (F.col("hotel_id") == ""),
        "Missing hotel_id"
    )
    .when(
        F.col("business_date").isNull(),
        "Missing business_date"
    )
    .when(
        F.col("rooms_sold").isNull() |
        (F.col("rooms_sold") < 0),
        "Invalid rooms_sold"
    )
    .when(
        F.col("rooms_available").isNull() |
        (F.col("rooms_available") <= 0),
        "Invalid rooms_available"
    )
    .when(
        F.col("rooms_sold") > F.col("rooms_available"),
        "Rooms sold exceeds rooms available"
    )
)

# Separate valid and invalid records
business_days_quarantine = business_days_checked.filter(
    F.col("invalid_reason").isNotNull()
)

business_days_valid = business_days_checked.filter(
    F.col("invalid_reason").isNull()
)

# Calculate occupancy percentage from room counts
business_days_valid = (
    business_days_valid
    .withColumn(
        "occupancy_pct",
        F.round(
            F.col("rooms_sold") / F.col("rooms_available") * 100,
            2
        )
    )
    .drop("invalid_reason")
    .dropDuplicates(["business_day_id"])
)

# Save Silver tables
(
    business_days_valid.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{silver}.business_days")
)

(
    business_days_quarantine.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{silver}.business_days_quarantine")
)

print("Valid business-day rows:", business_days_valid.count())
print("Quarantined rows:", business_days_quarantine.count())


# COMMAND ----------


from pyspark.sql import functions as F

silver = "hilton_catalog.silver"

reservations = spark.table(f"{silver}.reservations")
hotels = spark.table(f"{silver}.hotels")
guests = spark.table(f"{silver}.guests")
rooms = spark.table(f"{silver}.rooms")
room_types = spark.table(f"{silver}.room_types")
rate_plans = spark.table(f"{silver}.rate_plans")
payments = spark.table(f"{silver}.payments")
business_days = spark.table(f"{silver}.business_days")

def count_orphans(child_df, child_key, parent_df, parent_key):
    return (
        child_df.alias("c")
        .join(
            parent_df.select(parent_key).distinct().alias("p"),
            F.col(f"c.{child_key}") == F.col(f"p.{parent_key}"),
            "left_anti"
        )
        .count()
    )

checks = {
    "Reservations with unknown hotels": count_orphans(
        reservations, "hotel_id", hotels, "hotel_id"
    ),
    "Reservations with unknown guests": count_orphans(
        reservations, "guest_id", guests, "guest_id"
    ),
    "Reservations with unknown room types": count_orphans(
        reservations, "room_type_id", room_types, "room_type_id"
    ),
    "Reservations with unknown rate plans": count_orphans(
        reservations, "rate_plan_id", rate_plans, "rate_plan_id"
    ),
    "Rooms with unknown hotels": count_orphans(
        rooms, "hotel_id", hotels, "hotel_id"
    ),
    "Rooms with unknown room types": count_orphans(
        rooms, "room_type_id", room_types, "room_type_id"
    ),
    "Payments with unknown reservations": count_orphans(
        payments, "reservation_id", reservations, "reservation_id"
    ),
    "Business days with unknown hotels": count_orphans(
        business_days, "hotel_id", hotels, "hotel_id"
    )
}

for check_name, issue_count in checks.items():
    result = "PASS" if issue_count == 0 else "REVIEW"
    print(f"{result}: {check_name} = {issue_count}")


# COMMAND ----------

