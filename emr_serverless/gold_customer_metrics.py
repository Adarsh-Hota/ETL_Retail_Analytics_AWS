import sys

from pyspark.context import SparkContext

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_date,
    current_timestamp,
    datediff,
    dayofmonth,
    desc,
    max,
    min,
    month,
    row_number,
    sum,
    when,
    year
)

from pyspark.sql.window import Window


# ----------------------------------------------------------
# Job Initialization
# ----------------------------------------------------------

spark = (
    SparkSession
    .builder
    .appName("gold_customer_metrics")
    .getOrCreate()
)

# ----------------------------------------------------------
# Read Silver Layer
# ----------------------------------------------------------

customers_df = spark.read.parquet(
    "s3://retail-analytics-platform/silver/customers/"
)

orders_df = spark.read.parquet(
    "s3://retail-analytics-platform/silver/orders/"
)


# ----------------------------------------------------------
# Order Metrics
# ----------------------------------------------------------

order_metrics_df = (
    orders_df
    .groupBy(
        "customer_id"
    )
    .agg(

        count("*").alias(
            "total_orders"
        ),

        sum("quantity").alias(
            "total_quantity"
        ),

        sum("total_amount").alias(
            "total_revenue"
        ),

        avg("total_amount").alias(
            "average_order_value"
        ),

        max("total_amount").alias(
            "highest_order_value"
        ),

        min("total_amount").alias(
            "lowest_order_value"
        ),

        avg("quantity").alias(
            "average_order_quantity"
        ),

        min("order_timestamp").alias(
            "first_purchase_date"
        ),

        max("order_timestamp").alias(
            "last_purchase_date"
        )
    )
)


# ----------------------------------------------------------
# Favorite Category
# ----------------------------------------------------------

category_counts_df = (
    orders_df
    .groupBy(
        "customer_id",
        "category"
    )
    .agg(
        count("*").alias(
            "category_purchase_count"
        )
    )
)


category_window = Window.partitionBy(
    "customer_id"
).orderBy(
    desc("category_purchase_count"),
    col("category")
)


favorite_category_df = (
    category_counts_df
    .withColumn(
        "category_rank",
        row_number().over(
            category_window
        )
    )
    .filter(
        col("category_rank") == 1
    )
    .select(
        "customer_id",
        col("category").alias(
            "favorite_category"
        )
    )
)

# ----------------------------------------------------------
# Favorite Brand
# ----------------------------------------------------------

brand_counts_df = (
    orders_df
    .groupBy(
        "customer_id",
        "brand"
    )
    .agg(
        count("*").alias(
            "brand_purchase_count"
        )
    )
)


brand_window = Window.partitionBy(
    "customer_id"
).orderBy(
    desc("brand_purchase_count"),
    col("brand")
)


favorite_brand_df = (
    brand_counts_df
    .withColumn(
        "brand_rank",
        row_number().over(
            brand_window
        )
    )
    .filter(
        col("brand_rank") == 1
    )
    .select(
        "customer_id",
        col("brand").alias(
            "favorite_brand"
        )
    )
)


# ----------------------------------------------------------
# Most Recent Purchase
# ----------------------------------------------------------

latest_purchase_window = (
    Window
    .partitionBy(
        "customer_id"
    )
    .orderBy(
        desc("order_timestamp")
    )
)


latest_purchase_df = (
    orders_df
    .withColumn(
        "purchase_rank",
        row_number().over(
            latest_purchase_window
        )
    )
    .filter(
        col("purchase_rank") == 1
    )
    .select(
        "customer_id",

        col("category").alias(
            "most_recent_category"
        ),

        col("brand").alias(
            "most_recent_brand"
        ),

        col("order_timestamp").alias(
            "most_recent_purchase_timestamp"
        )
    )
)


# ----------------------------------------------------------
# Build Gold Customer Metrics
# ----------------------------------------------------------

customer_metrics_df = (
    customers_df

    .join(
        order_metrics_df,
        on="customer_id",
        how="left"
    )

    .join(
        favorite_category_df,
        on="customer_id",
        how="left"
    )

    .join(
        favorite_brand_df,
        on="customer_id",
        how="left"
    )

    .join(
        latest_purchase_df,
        on="customer_id",
        how="left"
    )
)


# ----------------------------------------------------------
# Replace Null Metrics
# ----------------------------------------------------------

customer_metrics_df = (
    customer_metrics_df

    .fillna(
        {
            "total_orders": 0,
            "total_quantity": 0,
            "total_revenue": 0,
            "average_order_value": 0,
            "highest_order_value": 0,
            "lowest_order_value": 0,
            "average_order_quantity": 0
        }
    )
)


# ----------------------------------------------------------
# Derived Business Metrics
# ----------------------------------------------------------

customer_metrics_df = (
    customer_metrics_df

    .withColumn(
        "days_since_last_purchase",
        when(
            col("last_purchase_date").isNull(),
            None
        ).otherwise(
            datediff(
                current_date(),
                col("last_purchase_date")
            )
        )
    )

    .withColumn(
        "customer_status",
        when(
            col("last_purchase_date").isNull(),
            "Never Purchased"
        )
        .when(
            col("days_since_last_purchase") <= 30,
            "Active"
        )
        .when(
            col("days_since_last_purchase") <= 90,
            "At Risk"
        )
        .otherwise(
            "Inactive"
        )
    )

    .withColumn(
        "customer_tenure_days",
        datediff(
            current_date(),
            col("signup_date")
        )
    )
)

# ----------------------------------------------------------
# Add ETL Metadata
# ----------------------------------------------------------

customer_metrics_df = (
    customer_metrics_df

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
# Select Final Columns
# ----------------------------------------------------------

customer_metrics_df = (
    customer_metrics_df.select(

        # Customer Identity
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "city",
        "state",
        "signup_date",
        "customer_tenure_days",
        "loyalty_tier",
        "preferred_category",

        # Metrics
        "total_orders",
        "total_quantity",
        "total_revenue",
        "average_order_value",
        "highest_order_value",
        "lowest_order_value",
        "average_order_quantity",

        # Dates
        "first_purchase_date",
        "last_purchase_date",
        "days_since_last_purchase",

        # Behaviour
        "favorite_category",
        "favorite_brand",
        "most_recent_category",
        "most_recent_brand",
        "most_recent_purchase_timestamp",

        # Business Status
        "customer_status",

        # ETL
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
    customer_metrics_df.write

    .mode("append")

    .partitionBy(
        "year",
        "month",
        "day"
    )

    .parquet(
        "s3://retail-analytics-platform/gold/customer_metrics/"
    )
)

spark.stop()
