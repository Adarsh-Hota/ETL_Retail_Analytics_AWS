from pyspark.sql import SparkSession

from pyspark.sql.window import Window

from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    current_date,
    current_timestamp,
    datediff,
    dayofmonth,
    lag,
    month,
    sum,
    to_date,
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
        "gold_daily_sales"
    )
    .getOrCreate()
)


# ----------------------------------------------------------
# Read Silver Layer
# ----------------------------------------------------------

orders_df = spark.read.parquet(
    "s3://retail-analytics-platform/silver/orders/"
)

products_df = spark.read.parquet(
    "s3://retail-analytics-platform/silver/products/"
)


# ----------------------------------------------------------
# Select Required Product Columns
# ----------------------------------------------------------

products_df = (
    products_df
    .select(
        "product_id",
        "cost_price",
        "launch_date",
        "is_active"
    )
)


# ----------------------------------------------------------
# Enrich Orders
# ----------------------------------------------------------

orders_df = (
    orders_df

    .join(
        products_df,
        on="product_id",
        how="left"
    )

    .withColumn(
        "sale_date",
        to_date(
            col("order_timestamp")
        )
    )
)


# ----------------------------------------------------------
# Daily Sales Metrics
# ----------------------------------------------------------

daily_sales_df = (
    orders_df

    .groupBy(
        "sale_date",
        "category",
        "brand"
    )

    .agg(

        count("*").alias(
            "total_orders"
        ),

        sum("quantity").alias(
            "units_sold"
        ),

        sum("total_amount").alias(
            "total_revenue"
        ),

        countDistinct(
            "customer_id"
        ).alias(
            "unique_customers"
        ),

        countDistinct(
            "product_id"
        ).alias(
            "unique_products"
        ),

        avg("total_amount").alias(
            "average_order_value"
        ),

        avg("quantity").alias(
            "average_order_quantity"
        ),

        sum(
            col("quantity")
            *
            col("cost_price")
        ).alias(
            "estimated_cost"
        ),

        countDistinct(

            when(
                col("is_active"),
                col("product_id")
            )

        ).alias(
            "active_products_sold"
        ),

        sum(

            when(

                datediff(
                    current_date(),
                    col("launch_date")
                ) <= 90,

                col("total_amount")

            ).otherwise(0)

        ).alias(
            "new_product_revenue"
        )
    )
)


# ----------------------------------------------------------
# Profitability
# ----------------------------------------------------------

daily_sales_df = (
    daily_sales_df

    .withColumn(
        "gross_profit",
        col("total_revenue")
        -
        col("estimated_cost")
    )

    .withColumn(
        "gross_margin_percent",

        when(
            col("total_revenue") == 0,
            0
        )

        .otherwise(

            (
                col("gross_profit")
                /
                col("total_revenue")
            )
            *
            100

        )
    )
)

# ----------------------------------------------------------
# Previous Day Revenue
# ----------------------------------------------------------

sales_window = (
    Window
    .partitionBy(
        "category",
        "brand"
    )
    .orderBy(
        "sale_date"
    )
)


daily_sales_df = (
    daily_sales_df

    .withColumn(
        "previous_day_revenue",
        lag(
            "total_revenue"
        ).over(
            sales_window
        )
    )
)


# ----------------------------------------------------------
# Daily Revenue Growth
# ----------------------------------------------------------

daily_sales_df = (
    daily_sales_df

    .withColumn(
        "daily_growth_percent",

        when(
            col("previous_day_revenue").isNull(),
            None
        )

        .when(
            col("previous_day_revenue") == 0,
            None
        )

        .otherwise(

            (
                (
                    col("total_revenue")
                    -
                    col("previous_day_revenue")
                )
                /
                col("previous_day_revenue")
            )
            *
            100

        )
    )
)


# ----------------------------------------------------------
# Sales Trend
# ----------------------------------------------------------

daily_sales_df = (
    daily_sales_df

    .withColumn(
        "sales_trend",

        when(
            col("daily_growth_percent").isNull(),
            "New"
        )

        .when(
            col("daily_growth_percent") >= 20,
            "High Growth"
        )

        .when(
            col("daily_growth_percent") >= 0,
            "Stable"
        )

        .otherwise(
            "Declining"
        )
    )
)


# ----------------------------------------------------------
# ETL Metadata
# ----------------------------------------------------------

daily_sales_df = (
    daily_sales_df

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

daily_sales_df = (
    daily_sales_df.select(

        "sale_date",

        "category",

        "brand",

        "total_orders",

        "units_sold",

        "total_revenue",

        "unique_customers",

        "unique_products",

        "average_order_value",

        "average_order_quantity",

        "estimated_cost",

        "gross_profit",

        "gross_margin_percent",

        "active_products_sold",

        "new_product_revenue",

        "previous_day_revenue",

        "daily_growth_percent",

        "sales_trend",

        "processed_timestamp",

        "year",

        "month",

        "day"
    )
)


# ----------------------------------------------------------
# Write Gold
# ----------------------------------------------------------

(
    daily_sales_df

    .write

    .mode("append")

    .partitionBy(
        "year",
        "month",
        "day"
    )

    .parquet(
        "s3://retail-analytics-platform/gold/daily_sales/"
    )
)


spark.stop()
