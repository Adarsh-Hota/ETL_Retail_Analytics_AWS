from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    current_date,
    current_timestamp,
    datediff,
    desc,
    lit,
    max,
    min,
    row_number,
    sum,
    when,
    year,
    month,
    dayofmonth
)

from pyspark.sql.window import Window


# ----------------------------------------------------------
# Spark Session
# ----------------------------------------------------------

spark = (
    SparkSession
    .builder
    .appName(
        "gold_product_performance"
    )
    .getOrCreate()
)


# ----------------------------------------------------------
# Read Silver Layer
# ----------------------------------------------------------

products_df = spark.read.parquet(
    "s3://retail-analytics-platform/silver/products/"
)

orders_df = spark.read.parquet(
    "s3://retail-analytics-platform/silver/orders/"
)

inventory_df = spark.read.parquet(
    "s3://retail-analytics-platform/silver/inventory/"
)


# ----------------------------------------------------------
# Sales Metrics
# ----------------------------------------------------------

sales_metrics_df = (
    orders_df

    .groupBy(
        "product_id"
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

        avg("quantity").alias(
            "average_order_quantity"
        ),

        countDistinct(
            "customer_id"
        ).alias(
            "unique_customers"
        ),

        min(
            "order_timestamp"
        ).alias(
            "first_sale_date"
        ),

        max(
            "order_timestamp"
        ).alias(
            "last_sale_date"
        )
    )
)


sales_metrics_df = (
    sales_metrics_df

    .withColumn(
        "days_since_last_sale",
        datediff(
            current_date(),
            col("last_sale_date")
        )
    )
)


# ----------------------------------------------------------
# Inventory Metrics
# ----------------------------------------------------------

inventory_metrics_df = (
    inventory_df

    .groupBy(
        "product_id"
    )

    .agg(

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
                col("movement_type") == "ADJUSTMENT",
                col("quantity_change")
            ).otherwise(0)
        ).alias(
            "adjustments"
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
            col("quantity_change")
        ).alias(
            "net_inventory_change"
        )
    )
)


# ----------------------------------------------------------
# Build Product Performance
# ----------------------------------------------------------

product_performance_df = (
    products_df

    .join(
        sales_metrics_df,
        on="product_id",
        how="left"
    )

    .join(
        inventory_metrics_df,
        on="product_id",
        how="left"
    )
)


# ----------------------------------------------------------
# Replace Null Metrics
# ----------------------------------------------------------

product_performance_df = (
    product_performance_df

    .fillna(
        {
            "total_orders": 0,
            "units_sold": 0,
            "total_revenue": 0,
            "average_order_quantity": 0,
            "unique_customers": 0,

            "stock_in": 0,
            "stock_out": 0,
            "adjustments": 0,
            "sales_inventory": 0,
            "net_inventory_change": 0
        }
    )
)


# ----------------------------------------------------------
# Profitability
# ----------------------------------------------------------

product_performance_df = (
    product_performance_df

    .withColumn(
        "estimated_cost",
        col("units_sold")
        *
        col("cost_price")
    )

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
        ).otherwise(

            (
                col("gross_profit")
                /
                col("total_revenue")
            )
            * 100

        )
    )
)


# ----------------------------------------------------------
# Average Product Revenue
# ----------------------------------------------------------

average_product_revenue = (
    product_performance_df
    .select(
        avg("total_revenue").alias(
            "average_product_revenue"
        )
    )
    .collect()[0][
        "average_product_revenue"
    ]
)


# ----------------------------------------------------------
# Product Status
# ----------------------------------------------------------

product_performance_df = (
    product_performance_df

    .withColumn(
        "average_product_revenue",
        lit(
            average_product_revenue
        )
    )

    .withColumn(
        "product_status",

        when(
            col("total_revenue") >=
            col("average_product_revenue") * 1.5,
            "Best Seller"
        )

        .when(
            col("total_revenue") >=
            col("average_product_revenue"),
            "Good Performer"
        )

        .otherwise(
            "Slow Moving"
        )
    )
)


# ----------------------------------------------------------
# Inventory Risk
# ----------------------------------------------------------

product_performance_df = (
    product_performance_df

    .withColumn(
        "inventory_risk",

        when(
            col("net_inventory_change") < -100,
            "Low Stock Risk"
        )

        .otherwise(
            "Healthy"
        )
    )
)


# ----------------------------------------------------------
# Data Quality Check
# ----------------------------------------------------------

product_performance_df = (
    product_performance_df

    .withColumn(
        "sales_vs_popularity",
        when(
            col("units_sold") > col("popularity_score") * 5,
            "Overperforming"
        )
        .when(
            col("units_sold") < col("popularity_score"),
            "Underperforming"
        )
        .otherwise(
            "On Track"
        )
    )
)


# ----------------------------------------------------------
# ETL Metadata
# ----------------------------------------------------------

product_performance_df = (
    product_performance_df

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

product_performance_df = (
    product_performance_df.select(

        # Product
        "product_id",
        "product_name",
        "category",
        "subcategory",
        "brand",

        "price",
        "cost_price",

        "launch_date",

        "is_active",

        "popularity_score",

        # Sales
        "total_orders",
        "units_sold",
        "total_revenue",
        "average_order_quantity",
        "unique_customers",

        "first_sale_date",
        "last_sale_date",
        "days_since_last_sale",

        # Inventory
        "stock_in",
        "stock_out",
        "adjustments",
        "sales_inventory",
        "net_inventory_change",

        # Profitability
        "estimated_cost",
        "gross_profit",
        "gross_margin_percent",

        # Classification
        "average_product_revenue",
        "product_status",
        "inventory_risk",

        # Data Quality
        "sales_vs_popularity",

        # Metadata
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
    product_performance_df

    .write

    .mode("append")

    .partitionBy(
        "year",
        "month",
        "day"
    )

    .parquet(
        "s3://retail-analytics-platform/gold/product_performance/"
    )
)

spark.stop()
