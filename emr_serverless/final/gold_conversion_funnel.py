from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    countDistinct,
    current_timestamp,
    dayofmonth,
    month,
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
        "gold_conversion_funnel"
    )
    .getOrCreate()
)


# ----------------------------------------------------------
# Read Silver Layer
# ----------------------------------------------------------

clickstream_df = spark.read.parquet(
    "s3://retail-analytics-platform/silver/clickstream/"
)


# ----------------------------------------------------------
# Funnel Metrics
# ----------------------------------------------------------

conversion_funnel_df = (
    clickstream_df

    .groupBy(
        "product_id"
    )

    .agg(

        sum(
            when(
                col("event_type") == "view_product",
                1
            ).otherwise(0)
        ).alias(
            "product_views"
        ),

        sum(
            when(
                col("event_type") == "add_to_cart",
                1
            ).otherwise(0)
        ).alias(
            "cart_additions"
        ),

        sum(
            when(
                col("event_type") == "checkout",
                1
            ).otherwise(0)
        ).alias(
            "checkout_starts"
        ),

        sum(
            when(
                col("event_type") == "purchase",
                1
            ).otherwise(0)
        ).alias(
            "purchases"
        ),

        countDistinct(
            "customer_id"
        ).alias(
            "unique_customers"
        ),

        countDistinct(
            "session_id"
        ).alias(
            "unique_sessions"
        )
    )
)


# ----------------------------------------------------------
# Derived Metrics
# ----------------------------------------------------------

conversion_funnel_df = (
    conversion_funnel_df

    .withColumn(
        "view_to_cart_rate",
        when(
            col("product_views") == 0,
            0
        ).otherwise(
            (
                col("cart_additions")
                /
                col("product_views")
            ) * 100
        )
    )

    .withColumn(
        "cart_to_checkout_rate",
        when(
            col("cart_additions") == 0,
            0
        ).otherwise(
            (
                col("checkout_starts")
                /
                col("cart_additions")
            ) * 100
        )
    )

    .withColumn(
        "checkout_to_purchase_rate",
        when(
            col("checkout_starts") == 0,
            0
        ).otherwise(
            (
                col("purchases")
                /
                col("checkout_starts")
            ) * 100
        )
    )

    .withColumn(
        "overall_conversion_rate",
        when(
            col("product_views") == 0,
            0
        ).otherwise(
            (
                col("purchases")
                /
                col("product_views")
            ) * 100
        )
    )

    .withColumn(
        "cart_abandonments",
        col("cart_additions")
        -
        col("checkout_starts")
    )

    .withColumn(
        "checkout_abandonments",
        col("checkout_starts")
        -
        col("purchases")
    )

    .withColumn(
        "funnel_status",
        when(
            col("overall_conversion_rate") >= 15,
            "Excellent"
        )
        .when(
            col("overall_conversion_rate") >= 8,
            "Good"
        )
        .otherwise(
            "Needs Attention"
        )
    )
)


# ----------------------------------------------------------
# ETL Metadata
# ----------------------------------------------------------

conversion_funnel_df = (
    conversion_funnel_df

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

conversion_funnel_df = (
    conversion_funnel_df.select(

        "product_id",

        "product_views",

        "cart_additions",

        "checkout_starts",

        "purchases",

        "unique_customers",

        "unique_sessions",

        "view_to_cart_rate",

        "cart_to_checkout_rate",

        "checkout_to_purchase_rate",

        "overall_conversion_rate",

        "cart_abandonments",

        "checkout_abandonments",

        "funnel_status",

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
    conversion_funnel_df

    .write

    .mode("append")

    .partitionBy(
        "year",
        "month",
        "day"
    )

    .parquet(
        "s3://retail-analytics-platform/gold/conversion_funnel/"
    )
)


spark.stop()
