from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    sum,
    count,
    when,
    col
)


spark = SparkSession.builder.appName(
    "InventoryHealthAnalytics"
).getOrCreate()


# Read Silver Inventory
df = spark.read.parquet(
    "s3://retail-analytics-platform/silver/inventory/"
)


# Inventory Aggregations
inventory_health_df = (
    df.groupBy(
        "product_id",
        "warehouse_id"
    )
    .agg(

        count("inventory_event_id").alias(
            "total_movements"
        ),

        sum("quantity_change").alias(
            "net_inventory_change"
        ),

        sum(
            when(
                col("quantity_change") > 0,
                col("quantity_change")
            ).otherwise(0)
        ).alias(
            "total_stock_in"
        ),

        sum(
            when(
                col("quantity_change") < 0,
                -col("quantity_change")
            ).otherwise(0)
        ).alias(
            "total_stock_out"
        )
    )
)


# Write Gold Layer
inventory_health_df.write.mode(
    "overwrite"
).parquet(
    "s3://retail-analytics-platform/gold/inventory_health/"
)


spark.stop()
