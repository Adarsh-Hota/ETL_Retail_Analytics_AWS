from pyspark.sql import SparkSession

from pyspark.sql.window import Window

from pyspark.sql.functions import (
    col,
    count,
    current_date,
    current_timestamp,
    datediff,
    dayofmonth,
    max,
    month,
    row_number,
    sum,
    when,
    year
)


# ----------------------------------------------------------
# Spark Session
# ----------------------------------------------------------

spark = (
    SparkSession
    .builder
    .appName(
        "gold_inventory_health"
    )
    .getOrCreate()
)


# ----------------------------------------------------------
# Read Silver Layer
# ----------------------------------------------------------

inventory_df = spark.read.parquet(
    "s3://retail-analytics-platform/silver/inventory/"
)

products_df = spark.read.parquet(
    "s3://retail-analytics-platform/silver/products/"
)


# ----------------------------------------------------------
# Select Required Product Columns
# ----------------------------------------------------------

products_df = (
    products_df.select(
        "product_id",
        "product_name",
        "category",
        "subcategory",
        "brand",
        "is_active"
    )
)


# ----------------------------------------------------------
# Inventory Metrics
# ----------------------------------------------------------

inventory_metrics_df = (
    inventory_df

    .groupBy(
        "product_id",
        "warehouse_id"
    )

    .agg(

        count("*").alias(
            "total_inventory_events"
        ),

        sum(

            when(
                col("movement_type") == "STOCK_IN",
                col("quantity_change")
            ).otherwise(0)

        ).alias(
            "stock_in"
        ),

        sum(

            when(
                col("movement_type") == "STOCK_OUT",
                -col("quantity_change")
            ).otherwise(0)

        ).alias(
            "stock_out"
        ),

        sum(

            when(
                col("movement_type") == "SALE",
                -col("quantity_change")
            ).otherwise(0)

        ).alias(
            "sales_inventory"
        ),

        sum(

            when(
                col("movement_type") == "ADJUSTMENT",
                col("quantity_change")
            ).otherwise(0)

        ).alias(
            "adjustments"
        ),

        sum(
            col("quantity_change")
        ).alias(
            "net_inventory_change"
        ),

        max(
            "event_timestamp"
        ).alias(
            "last_inventory_event"
        )
    )
)


# ----------------------------------------------------------
# Days Since Last Inventory Event
# ----------------------------------------------------------

inventory_metrics_df = (
    inventory_metrics_df

    .withColumn(
        "days_since_last_inventory_event",

        datediff(
            current_date(),
            col("last_inventory_event")
        )
    )
)


# ----------------------------------------------------------
# Build Inventory Health
# ----------------------------------------------------------

inventory_health_df = (
    inventory_metrics_df

    .join(
        products_df,
        on="product_id",
        how="left"
    )
)

# ----------------------------------------------------------
# Total Outbound
# ----------------------------------------------------------

inventory_health_df = (
    inventory_health_df

    .withColumn(
        "total_outbound",
        col("stock_out")
        +
        col("sales_inventory")
    )
)


# ----------------------------------------------------------
# Inventory Consumption Ratio
# ----------------------------------------------------------

inventory_health_df = (
    inventory_health_df

    .withColumn(

        "inventory_consumption_ratio",

        when(
            col("stock_in") == 0,
            None
        )

        .otherwise(

            col("total_outbound")
            /
            col("stock_in")

        )
    )
)


# ----------------------------------------------------------
# Inventory Status
# ----------------------------------------------------------

inventory_health_df = (
    inventory_health_df

    .withColumn(

        "inventory_status",

        when(
            col("inventory_consumption_ratio").isNull(),
            "Unknown"
        )

        .when(
            col("inventory_consumption_ratio") >= 1,
            "Critical"
        )

        .when(
            col("inventory_consumption_ratio") >= 0.7,
            "Watch"
        )

        .otherwise(
            "Healthy"
        )
    )
)


# ----------------------------------------------------------
# Warehouse Activity Rank
# ----------------------------------------------------------

warehouse_window = (
    Window

    .partitionBy(
        "product_id"
    )

    .orderBy(
        col("total_inventory_events").desc()
    )
)


inventory_health_df = (
    inventory_health_df

    .withColumn(

        "warehouse_activity_rank",

        row_number().over(
            warehouse_window
        )
    )
)


# ----------------------------------------------------------
# ETL Metadata
# ----------------------------------------------------------

inventory_health_df = (
    inventory_health_df

    .withColumn(
        "processed_timestamp",
        current_timestamp()
    )

    .withColumn(
        "year",
        year(
            col("processed_timestamp")
        )
    )

    .withColumn(
        "month",
        month(
            col("processed_timestamp")
        )
    )

    .withColumn(
        "day",
        dayofmonth(
            col("processed_timestamp")
        )
    )
)


# ----------------------------------------------------------
# Final Columns
# ----------------------------------------------------------

inventory_health_df = (
    inventory_health_df.select(

        # Product
        "product_id",
        "product_name",
        "category",
        "subcategory",
        "brand",
        "is_active",

        # Warehouse
        "warehouse_id",

        # Inventory
        "stock_in",
        "stock_out",
        "sales_inventory",
        "adjustments",
        "total_outbound",
        "net_inventory_change",

        # Activity
        "total_inventory_events",
        "last_inventory_event",
        "days_since_last_inventory_event",

        # Health
        "inventory_consumption_ratio",
        "inventory_status",
        "warehouse_activity_rank",

        # Metadata
        "processed_timestamp",
        "year",
        "month",
        "day"
    )
)


# ----------------------------------------------------------
# Write Gold Layer
# ----------------------------------------------------------

(
    inventory_health_df

    .write

    .mode("append")

    .partitionBy(
        "year",
        "month",
        "day"
    )

    .parquet(
        "s3://retail-analytics-platform/gold/inventory_health/"
    )
)


spark.stop()
