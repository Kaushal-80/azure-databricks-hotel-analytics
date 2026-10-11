# Databricks notebook source

from pyspark.sql import functions as F

reservations = spark.table("hilton_catalog.silver.reservations")
business_days = spark.table("hilton_catalog.silver.business_days")
hotels = spark.table("hilton_catalog.silver.hotels")

print("=== Reservation status counts ===")
display(
    reservations.groupBy("status")
    .count()
    .orderBy(F.desc("count"))
)

print("=== Reservation date range ===")
display(
    reservations.agg(
        F.min("check_in_date").alias("earliest_check_in"),
        F.max("check_in_date").alias("latest_check_in"),
        F.min("check_out_date").alias("earliest_check_out"),
        F.max("check_out_date").alias("latest_check_out"),
        F.min("booking_date").alias("earliest_booking"),
        F.max("booking_date").alias("latest_booking")
    )
)

print("=== Business-day date range ===")
display(
    business_days.agg(
        F.min("business_date").alias("first_business_date"),
        F.max("business_date").alias("last_business_date"),
        F.count("*").alias("total_business_day_records"),
        F.countDistinct("hotel_id").alias("number_of_hotels")
    )
)

print("=== Daily occupancy statistics ===")
display(
    business_days.select(
        "occupancy",
        "occupancy_pct",
        "rooms_sold",
        "rooms_available"
    ).summary()
)

print("=== Hotel details ===")
display(hotels)


# COMMAND ----------


from pyspark.sql import functions as F

# Read Silver tables
business_days = spark.table("hilton_catalog.silver.business_days")
hotels = spark.table("hilton_catalog.silver.hotels")

# Join daily operational data with hotel details
hotel_daily_occupancy = (
    business_days.alias("bd")
    .join(
        hotels.alias("h"),
        F.col("bd.hotel_id") == F.col("h.hotel_id"),
        "left"
    )
    .select(
        F.col("bd.business_day_id"),
        F.col("bd.hotel_id"),
        F.col("h.hotel_name"),
        F.col("h.city"),
        F.col("h.state"),
        F.col("h.country"),
        F.col("bd.business_date"),
        F.col("bd.rooms_sold"),
        F.col("bd.rooms_available"),
        F.round(
            F.col("bd.rooms_sold") * 100.0 /
            F.col("bd.rooms_available"),
            2
        ).alias("occupancy_pct"),
        F.current_timestamp().alias("_gold_created_at")
    )
)

# Save as a managed Delta table in the Gold schema
(
    hotel_daily_occupancy.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hilton_catalog.gold.hotel_daily_occupancy")
)

print("Gold hotel daily occupancy table created successfully.")

display(
    spark.table("hilton_catalog.gold.hotel_daily_occupancy")
    .orderBy("hotel_id", "business_date")
)


# COMMAND ----------


gold_occupancy = spark.table(
    "hilton_catalog.gold.hotel_daily_occupancy"
)

print("Total Gold records:", gold_occupancy.count())
print(
    "Distinct hotels:",
    gold_occupancy.select("hotel_id").distinct().count()
)
print(
    "Duplicate hotel/date records:",
    gold_occupancy.groupBy("hotel_id", "business_date")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

display(
    gold_occupancy.groupBy("hotel_id", "hotel_name")
    .agg(
        F.sum("rooms_sold").alias("total_rooms_sold"),
        F.sum("rooms_available").alias("total_rooms_available"),
        F.round(
            F.sum("rooms_sold") * 100.0 /
            F.sum("rooms_available"),
            2
        ).alias("weighted_occupancy_pct"),
        F.countDistinct("business_date").alias("days_reported")
    )
    .orderBy("hotel_id")
)


# COMMAND ----------


from pyspark.sql import functions as F

# Read Silver tables
reservations = spark.table("hilton_catalog.silver.reservations")
hotels = spark.table("hilton_catalog.silver.hotels")
guests = spark.table("hilton_catalog.silver.guests")
room_types = spark.table("hilton_catalog.silver.room_types")
rate_plans = spark.table("hilton_catalog.silver.rate_plans")

# Join reservation data with reference information
reservation_analytics = (
    reservations.alias("r")
    .join(
        hotels.alias("h"),
        F.col("r.hotel_id") == F.col("h.hotel_id"),
        "left"
    )
    .join(
        guests.alias("g"),
        F.col("r.guest_id") == F.col("g.guest_id"),
        "left"
    )
    .join(
        room_types.alias("rt"),
        F.col("r.room_type_id") == F.col("rt.room_type_id"),
        "left"
    )
    .join(
        rate_plans.alias("rp"),
        F.col("r.rate_plan_id") == F.col("rp.rate_plan_id"),
        "left"
    )
    .select(
        F.col("r.reservation_id"),
        F.col("r.hotel_id"),
        F.col("h.hotel_name"),
        F.col("h.city"),
        F.col("h.country"),
        F.col("r.guest_id"),
        F.col("g.country").alias("guest_country"),
        F.col("g.loyalty_id"),
        F.col("r.room_type_id"),
        F.col("rt.room_type_name"),
        F.col("r.rate_plan_id"),
        F.col("rp.rate_plan_name"),
        F.col("rp.meal_plan"),
        F.col("rp.refundable"),
        F.col("r.booking_date"),
        F.col("r.check_in_date"),
        F.col("r.check_out_date"),
        F.datediff(
            F.col("r.check_out_date"),
            F.col("r.check_in_date")
        ).alias("length_of_stay"),
        F.col("r.status").alias("reservation_status"),
        F.col("r.adults"),
        F.col("r.children"),
        F.col("r.rooms"),
        F.col("r.total_amount").alias("reservation_value"),
        F.col("r.payment_status"),
        F.current_timestamp().alias("_gold_created_at")
    )
)

# Write the Gold table
(
    reservation_analytics.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hilton_catalog.gold.reservation_analytics")
)

print("Gold reservation analytics table created successfully.")

display(
    spark.table("hilton_catalog.gold.reservation_analytics").limit(20)
)


# COMMAND ----------


gold_reservations = spark.table(
    "hilton_catalog.gold.reservation_analytics"
)

print("Total Gold reservations:", gold_reservations.count())

print(
    "Distinct reservation IDs:",
    gold_reservations.select("reservation_id").distinct().count()
)

print(
    "Duplicate reservation IDs:",
    gold_reservations.groupBy("reservation_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print("=== Reservation status summary ===")
display(
    gold_reservations.groupBy("reservation_status")
    .agg(
        F.count("*").alias("reservation_count"),
        F.round(
            F.sum("reservation_value"), 2
        ).alias("total_reservation_value")
    )
    .orderBy(F.desc("reservation_count"))
)


# COMMAND ----------


from pyspark.sql import functions as F

gold_reservations = spark.table(
    "hilton_catalog.gold.reservation_analytics"
)

hotel_revenue_summary = (
    gold_reservations
    .groupBy("hotel_id", "hotel_name", "city", "country")
    .agg(
        F.countDistinct("reservation_id").alias("total_reservations"),

        F.sum(
            F.when(
                F.col("reservation_status") == "CHECKED_OUT", 1
            ).otherwise(0)
        ).alias("completed_stays"),

        F.sum("reservation_value").alias("total_reservation_value"),

        F.sum(
            F.when(
                F.col("reservation_status") == "CHECKED_OUT",
                F.col("reservation_value")
            ).otherwise(0)
        ).alias("completed_stay_value"),

        F.avg("reservation_value").alias("avg_reservation_value"),

        F.sum("rooms").alias("total_rooms_booked")
    )
    .withColumn(
        "total_reservation_value",
        F.round("total_reservation_value", 2)
    )
    .withColumn(
        "completed_stay_value",
        F.round("completed_stay_value", 2)
    )
    .withColumn(
        "avg_reservation_value",
        F.round("avg_reservation_value", 2)
    )
    .withColumn(
        "_gold_created_at",
        F.current_timestamp()
    )
)

(
    hotel_revenue_summary.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("hilton_catalog.gold.hotel_revenue_summary")
)

print("Gold hotel revenue summary created successfully.")

display(
    spark.table("hilton_catalog.gold.hotel_revenue_summary")
    .orderBy("hotel_id")
)


# COMMAND ----------


gold_summary = spark.table(
    "hilton_catalog.gold.hotel_revenue_summary"
)

print("Total hotel summary records:", gold_summary.count())
print(
    "Distinct hotels:",
    gold_summary.select("hotel_id").distinct().count()
)

print(
    "Total reservations across hotels:",
    gold_summary.agg(
        F.sum("total_reservations").alias("total")
    ).first()["total"]
)

print(
    "Total reservation value:",
    gold_summary.agg(
        F.round(F.sum("total_reservation_value"), 2).alias("total")
    ).first()["total"]
)

display(gold_summary)


# COMMAND ----------


from pyspark.sql import functions as F

reservations = spark.table(
    "hilton_catalog.gold.reservation_analytics"
)
summary = spark.table(
    "hilton_catalog.gold.hotel_revenue_summary"
)
occupancy = spark.table(
    "hilton_catalog.gold.hotel_daily_occupancy"
)

# 1. Reconcile reservation value between Gold tables
reservation_total = reservations.agg(
    F.round(F.sum("reservation_value"), 2).alias("total")
).first()["total"]

summary_total = summary.agg(
    F.round(F.sum("total_reservation_value"), 2).alias("total")
).first()["total"]

print("Reservation-level total:", reservation_total)
print("Hotel-summary total:", summary_total)
print("Values reconcile:", reservation_total == summary_total)

# 2. Check missing key fields
print(
    "Reservations missing reservation ID:",
    reservations.filter(F.col("reservation_id").isNull()).count()
)

print(
    "Reservations missing hotel ID:",
    reservations.filter(F.col("hotel_id").isNull()).count()
)

print(
    "Reservations with negative value:",
    reservations.filter(F.col("reservation_value") < 0).count()
)

# 3. Check occupancy metrics
print(
    "Occupancy records with invalid room counts:",
    occupancy.filter(
        (F.col("rooms_available").isNull()) |
        (F.col("rooms_available") <= 0) |
        (F.col("rooms_sold").isNull()) |
        (F.col("rooms_sold") < 0) |
        (F.col("rooms_sold") > F.col("rooms_available"))
    ).count()
)

print(
    "Occupancy percentages outside 0-100:",
    occupancy.filter(
        (F.col("occupancy_pct") < 0) |
        (F.col("occupancy_pct") > 100) |
        F.col("occupancy_pct").isNull()
    ).count()
)

# 4. Review reservation statuses
display(
    reservations.groupBy("reservation_status")
    .agg(
        F.count("*").alias("reservation_count"),
        F.round(F.sum("reservation_value"), 2)
        .alias("reservation_value")
    )
    .orderBy(F.desc("reservation_count"))
)


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 'hotel_daily_occupancy' AS table_name,
# MAGIC        COUNT(*) AS total_records
# MAGIC FROM hilton_catalog.gold.hotel_daily_occupancy
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'reservation_analytics',
# MAGIC        COUNT(*)
# MAGIC FROM hilton_catalog.gold.reservation_analytics
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'hotel_revenue_summary',
# MAGIC        COUNT(*)
# MAGIC FROM hilton_catalog.gold.hotel_revenue_summary;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     (SELECT ROUND(SUM(reservation_value), 2)
# MAGIC      FROM hilton_catalog.gold.reservation_analytics)
# MAGIC         AS reservation_level_total,
# MAGIC
# MAGIC     (SELECT ROUND(SUM(total_reservation_value), 2)
# MAGIC      FROM hilton_catalog.gold.hotel_revenue_summary)
# MAGIC         AS hotel_summary_total;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC     COUNT(DISTINCT reservation_id) AS unique_reservations
# MAGIC FROM hilton_catalog.gold.reservation_analytics;
# MAGIC

# COMMAND ----------

