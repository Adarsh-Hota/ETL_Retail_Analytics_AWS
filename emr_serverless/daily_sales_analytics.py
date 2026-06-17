from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    sum,
    count,
    avg,
    countDistinct,
    to_date,
    col
)


spark = SparkSession.builder.appName(
    "DailySalesAnalytics"
).getOrCreate()


# Read Silver Orders
df = spark.read.parquet(
    "s3://retail-analytics-platform/silver/orders/"
)


# Extract Order Date
df = df.withColumn(
    "order_date",
    to_date(
        col("order_timestamp")
    )
)


# Daily Aggregations
daily_sales_df = (
    df.groupBy("order_date")
    .agg(

        count("order_id").alias(
            "total_orders"
        ),

        sum("total_amount").alias(
            "total_revenue"
        ),

        avg("total_amount").alias(
            "avg_order_value"
        ),

        countDistinct(
            "customer_id"
        ).alias(
            "unique_customers"
        )
    )
)


# Write Gold Layer
daily_sales_df.write.mode(
    "overwrite"
).parquet(
    "s3://retail-analytics-platform/gold/daily_sales/"
)


spark.stop()
