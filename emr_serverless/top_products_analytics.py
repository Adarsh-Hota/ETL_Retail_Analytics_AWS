from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    sum,
    count,
    avg,
    col
)


spark = SparkSession.builder.appName(
    "TopProductsAnalytics"
).getOrCreate()


# Read Silver Orders
df = spark.read.parquet(
    "s3://retail-analytics-platform/silver/orders/"
)


# Product Aggregations
top_products_df = (
    df.groupBy("product_id")
    .agg(

        count("order_id").alias(
            "total_orders"
        ),

        sum("quantity").alias(
            "total_quantity_sold"
        ),

        sum("total_amount").alias(
            "total_revenue"
        ),

        avg("unit_price").alias(
            "avg_selling_price"
        )
    )
)


# Write Gold Layer
top_products_df.write.mode(
    "overwrite"
).parquet(
    "s3://retail-analytics-platform/gold/top_products/"
)


spark.stop()
