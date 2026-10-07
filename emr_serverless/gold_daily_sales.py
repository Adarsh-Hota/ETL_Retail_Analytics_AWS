"""Build the daily sales Gold mart from current Iceberg Silver tables.

This job intentionally uses the current/latest-state Orders and Payments tables and
the current Products SCD2 version. Consequently, a row describes the source state
available at execution time, not a historical reconstruction as of ``sales_date``.
For example, a product category changed after an order was placed is attributed to
the product's current category when this mart is refreshed.
"""

import argparse
import logging
import re
from datetime import date

from pyspark.sql import DataFrame, SparkSession, functions as F


LOGGER = logging.getLogger(__name__)
DATE_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}")
COMPLETED_ORDER_STATUSES = ("DELIVERED", "COMPLETED")


def valid_iso_date(value: str) -> str:
    """Return a strictly formatted, real ISO calendar date for argparse."""
    if not DATE_PATTERN.fullmatch(value):
        raise argparse.ArgumentTypeError("must be a real ISO date in YYYY-MM-DD format")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "must be a real ISO date in YYYY-MM-DD format"
        ) from exc
    if parsed.isoformat() != value:
        raise argparse.ArgumentTypeError("must be a real ISO date in YYYY-MM-DD format")
    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build an Iceberg daily sales mart")
    parser.add_argument("--catalog_name", required=True)
    parser.add_argument("--silver_database", required=True)
    parser.add_argument("--gold_database", required=True)
    parser.add_argument("--gold_table", required=True)
    parser.add_argument("--sales_date", required=True, type=valid_iso_date)
    return parser.parse_args()


def build_daily_sales(
    orders: DataFrame,
    payments: DataFrame,
    products: DataFrame,
    sales_date: str,
) -> DataFrame:
    """Aggregate each source separately so payment attempts cannot multiply sales."""
    current_products = (
        products.filter(F.col("is_current") == F.lit(True))
        .select("product_id", "category")
        .dropDuplicates(["product_id"])
    )

    daily_orders = (
        orders.withColumn("sales_date", F.to_date(F.col("order_timestamp")))
        .filter(F.col("sales_date") == F.lit(sales_date).cast("date"))
        .withColumn("normalized_order_status", F.upper(F.trim(F.col("order_status"))))
        .join(current_products, on="product_id", how="left")
        .withColumn("category", F.coalesce(F.col("category"), F.lit("UNKNOWN")))
        .select(
            "sales_date",
            "category",
            "order_id",
            "customer_id",
            "quantity",
            "total_amount",
            "normalized_order_status",
        )
    )

    completed = F.col("normalized_order_status").isin(*COMPLETED_ORDER_STATUSES)
    cancelled = F.col("normalized_order_status") == F.lit("CANCELLED")
    order_metrics = daily_orders.groupBy("sales_date", "category").agg(
        F.count(F.lit(1)).cast("long").alias("total_orders"),
        F.sum(F.when(completed, F.lit(1)).otherwise(F.lit(0))).cast("long").alias(
            "completed_orders"
        ),
        F.sum(F.when(cancelled, F.lit(1)).otherwise(F.lit(0))).cast("long").alias(
            "cancelled_orders"
        ),
        F.countDistinct(F.when(completed, F.col("customer_id")))
        .cast("long")
        .alias("unique_purchasing_customers"),
        F.sum(F.when(completed, F.col("quantity").cast("long")).otherwise(F.lit(0)))
        .cast("long")
        .alias("units_sold"),
        F.sum(
            F.when(completed, F.col("total_amount").cast("double")).otherwise(F.lit(0.0))
        )
        .cast("double")
        .alias("gross_revenue"),
    )

    # Join the distinct daily order population to payments before aggregation. This
    # preserves multiple payment attempts without multiplying order revenue.
    daily_order_population = daily_orders.select("order_id", "sales_date", "category").dropDuplicates(
        ["order_id"]
    )
    payment_metrics = (
        payments.withColumn("normalized_payment_status", F.upper(F.trim(F.col("payment_status"))))
        .join(daily_order_population, on="order_id", how="inner")
        .groupBy("sales_date", "category")
        .agg(
            F.count(F.lit(1)).cast("long").alias("payment_attempts"),
            F.sum(
                F.when(F.col("normalized_payment_status") == "CAPTURED", F.lit(1)).otherwise(
                    F.lit(0)
                )
            )
            .cast("long")
            .alias("captured_payments"),
            F.sum(
                F.when(F.col("normalized_payment_status") == "FAILED", F.lit(1)).otherwise(F.lit(0))
            )
            .cast("long")
            .alias("failed_payments"),
            F.sum(
                F.when(F.col("normalized_payment_status") == "REFUNDED", F.lit(1)).otherwise(
                    F.lit(0)
                )
            )
            .cast("long")
            .alias("refunded_payments"),
            F.sum(
                F.when(
                    F.col("normalized_payment_status") == "CAPTURED",
                    F.col("amount").cast("double"),
                ).otherwise(F.lit(0.0))
            )
            .cast("double")
            .alias("captured_amount"),
            F.sum(
                F.when(
                    F.col("normalized_payment_status") == "REFUNDED",
                    F.col("amount").cast("double"),
                ).otherwise(F.lit(0.0))
            )
            .cast("double")
            .alias("refunded_amount"),
        )
    )

    zero_long = F.lit(0).cast("long")
    zero_double = F.lit(0.0).cast("double")
    combined = order_metrics.join(payment_metrics, on=["sales_date", "category"], how="left")
    combined = combined.select(
        "sales_date",
        "category",
        F.col("total_orders").cast("long").alias("total_orders"),
        F.col("completed_orders").cast("long").alias("completed_orders"),
        F.col("cancelled_orders").cast("long").alias("cancelled_orders"),
        F.col("unique_purchasing_customers").cast("long").alias("unique_purchasing_customers"),
        F.col("units_sold").cast("long").alias("units_sold"),
        F.col("gross_revenue").cast("double").alias("gross_revenue"),
        F.coalesce(F.col("payment_attempts"), zero_long).alias("payment_attempts"),
        F.coalesce(F.col("captured_payments"), zero_long).alias("captured_payments"),
        F.coalesce(F.col("failed_payments"), zero_long).alias("failed_payments"),
        F.coalesce(F.col("refunded_payments"), zero_long).alias("refunded_payments"),
        F.coalesce(F.col("captured_amount"), zero_double).alias("captured_amount"),
        F.coalesce(F.col("refunded_amount"), zero_double).alias("refunded_amount"),
    )

    return (
        combined.withColumn(
            "average_order_value",
            F.when(
                F.col("completed_orders") > F.lit(0),
                F.col("gross_revenue") / F.col("completed_orders"),
            ).otherwise(zero_double),
        )
        .withColumn(
            "payment_capture_rate",
            F.when(
                F.col("payment_attempts") > F.lit(0),
                F.col("captured_payments").cast("double") / F.col("payment_attempts"),
            ).otherwise(zero_double),
        )
        .withColumn("processed_timestamp", F.current_timestamp())
    )


def create_target_table(spark: SparkSession, table_name: str) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {table_name.rsplit('.', 1)[0]}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            sales_date DATE,
            category STRING,
            total_orders BIGINT,
            completed_orders BIGINT,
            cancelled_orders BIGINT,
            unique_purchasing_customers BIGINT,
            units_sold BIGINT,
            gross_revenue DOUBLE,
            average_order_value DOUBLE,
            payment_attempts BIGINT,
            captured_payments BIGINT,
            failed_payments BIGINT,
            refunded_payments BIGINT,
            captured_amount DOUBLE,
            refunded_amount DOUBLE,
            payment_capture_rate DOUBLE,
            processed_timestamp TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (sales_date)
        """
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    spark = SparkSession.builder.appName("gold_daily_sales").getOrCreate()

    try:
        orders_table = f"{args.catalog_name}.{args.silver_database}.silver_orders"
        payments_table = f"{args.catalog_name}.{args.silver_database}.silver_payments"
        products_table = f"{args.catalog_name}.{args.silver_database}.silver_products"
        gold_table = f"{args.catalog_name}.{args.gold_database}.{args.gold_table}"

        orders = spark.table(orders_table)
        payments = spark.table(payments_table)
        products = spark.table(products_table)
        LOGGER.info("Source count: orders=%s", orders.count())
        LOGGER.info("Source count: payments=%s", payments.count())
        LOGGER.info("Source count: products=%s", products.count())

        daily_sales = build_daily_sales(orders, payments, products, args.sales_date)
        output_count = daily_sales.count()
        LOGGER.info("Daily Sales output rows for %s: %s", args.sales_date, output_count)

        create_target_table(spark, gold_table)
        # Predicate overwrite replaces exactly this date's partition. It also clears
        # stale rows when no Orders match, while retaining every other sales date.
        daily_sales.writeTo(gold_table).overwrite(
            F.col("sales_date") == F.lit(args.sales_date).cast("date")
        )
        LOGGER.info("Completed Daily Sales refresh for sales_date=%s", args.sales_date)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
