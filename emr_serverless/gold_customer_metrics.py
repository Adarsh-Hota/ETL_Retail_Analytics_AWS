import argparse
import logging
import re
from datetime import date

from pyspark.sql import SparkSession, Window, functions as F


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("gold_customer_metrics")


def snapshot_date_argument(value):
    """Strictly validate the deterministic business reference date."""
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise argparse.ArgumentTypeError("snapshot_date must use YYYY-MM-DD format")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("snapshot_date must be a valid ISO date") from error
    if parsed.isoformat() != value:
        raise argparse.ArgumentTypeError("snapshot_date must use YYYY-MM-DD format")
    return value


def parse_arguments():
    parser = argparse.ArgumentParser(description="Build the Customer Metrics Gold snapshot")
    parser.add_argument("--catalog_name", required=True)
    parser.add_argument("--silver_database", required=True)
    parser.add_argument("--gold_database", required=True)
    parser.add_argument("--gold_table", required=True)
    parser.add_argument("--snapshot_date", required=True, type=snapshot_date_argument)
    return parser.parse_args()


def build_customer_metrics(spark, catalog_name, silver_database, snapshot_date):
    """Aggregate Orders and Payments independently before joining current Customers."""
    customers_table = f"{catalog_name}.{silver_database}.silver_customers"
    orders_table = f"{catalog_name}.{silver_database}.silver_orders"
    payments_table = f"{catalog_name}.{silver_database}.silver_payments"
    products_table = f"{catalog_name}.{silver_database}.silver_products"

    customers_df = spark.table(customers_table)
    orders_df = spark.table(orders_table)
    payments_df = spark.table(payments_table)
    products_df = spark.table(products_table)
    logger.info(
        "Source row counts: customers=%s orders=%s payments=%s products=%s",
        customers_df.count(), orders_df.count(), payments_df.count(), products_df.count(),
    )

    # Deliberately omit first_name, last_name, and email from every Gold path.
    current_customers = customers_df.filter(F.col("is_current") == F.lit(True)).select(
        "customer_id", "city", "state", "signup_date", "loyalty_tier", "preferred_category"
    )
    current_products = products_df.filter(F.col("is_current") == F.lit(True)).select(
        "product_id", "category", "brand"
    )
    normalized_orders = orders_df.withColumn(
        "_order_status", F.upper(F.trim(F.col("order_status")))
    )
    # The simulator emits Delivered; Completed remains valid for a future source.
    completed_orders = normalized_orders.filter(
        F.col("_order_status").isin("DELIVERED", "COMPLETED")
    ).select(
        "order_id", "customer_id", "product_id", "quantity", "total_amount", "order_timestamp"
    )
    completed_orders_enriched = completed_orders.join(current_products, "product_id", "left")

    order_metrics = completed_orders.groupBy("customer_id").agg(
        F.count("order_id").alias("completed_orders"),
        F.sum("quantity").alias("units_purchased"),
        F.sum("total_amount").cast("double").alias("gross_revenue"),
        F.min("order_timestamp").alias("first_purchase_timestamp"),
        F.max("order_timestamp").alias("last_purchase_timestamp"),
    )

    category_counts = (
        completed_orders_enriched.filter(F.col("category").isNotNull())
        .groupBy("customer_id", "category")
        .agg(F.count("order_id").alias("_order_count"))
    )
    favorite_category = (
        category_counts.withColumn(
            "_row_number",
            F.row_number().over(
                Window.partitionBy("customer_id").orderBy(F.col("_order_count").desc(), F.col("category").asc())
            ),
        )
        .filter(F.col("_row_number") == 1)
        .select("customer_id", F.col("category").alias("favorite_category"))
    )

    brand_counts = (
        completed_orders_enriched.filter(F.col("brand").isNotNull())
        .groupBy("customer_id", "brand")
        .agg(F.count("order_id").alias("_order_count"))
    )
    favorite_brand = (
        brand_counts.withColumn(
            "_row_number",
            F.row_number().over(
                Window.partitionBy("customer_id").orderBy(F.col("_order_count").desc(), F.col("brand").asc())
            ),
        )
        .filter(F.col("_row_number") == 1)
        .select("customer_id", F.col("brand").alias("favorite_brand"))
    )

    # Timestamp descending then order_id ascending deterministically chooses the
    # most recent completed purchase when timestamps are equal.
    most_recent_purchase = (
        completed_orders_enriched.withColumn(
            "_row_number",
            F.row_number().over(
                Window.partitionBy("customer_id").orderBy(
                    F.col("order_timestamp").desc(), F.col("order_id").asc()
                )
            ),
        )
        .filter(F.col("_row_number") == 1)
        .select(
            "customer_id",
            F.col("category").alias("most_recent_category"),
            F.col("brand").alias("most_recent_brand"),
        )
    )

    # Payment state has no customer_id. Map it through the latest-state Orders
    # bridge first, then aggregate once by customer to prevent join multiplication.
    order_customer_bridge = orders_df.select("order_id", "customer_id").dropDuplicates(["order_id"])
    normalized_payments = payments_df.withColumn(
        "_payment_status", F.upper(F.trim(F.col("payment_status")))
    )
    customer_payments = normalized_payments.join(order_customer_bridge, "order_id", "inner")
    payment_metrics = customer_payments.groupBy("customer_id").agg(
        F.count("payment_id").alias("payment_attempts"),
        F.sum(F.when(F.col("_payment_status") == "CAPTURED", F.lit(1)).otherwise(F.lit(0))).alias("captured_payments"),
        F.sum(F.when(F.col("_payment_status") == "FAILED", F.lit(1)).otherwise(F.lit(0))).alias("failed_payments"),
        F.sum(F.when(F.col("_payment_status") == "REFUNDED", F.lit(1)).otherwise(F.lit(0))).alias("refunded_payments"),
        F.sum(F.when(F.col("_payment_status") == "CAPTURED", F.col("amount")).otherwise(F.lit(0.0))).cast("double").alias("captured_amount"),
        F.sum(F.when(F.col("_payment_status") == "REFUNDED", F.col("amount")).otherwise(F.lit(0.0))).cast("double").alias("refunded_amount"),
    )

    snapshot_reference = F.lit(snapshot_date).cast("date")
    return (
        current_customers
        .join(order_metrics, "customer_id", "left")
        .join(favorite_category, "customer_id", "left")
        .join(favorite_brand, "customer_id", "left")
        .join(most_recent_purchase, "customer_id", "left")
        .join(payment_metrics, "customer_id", "left")
        .select(
            snapshot_reference.alias("snapshot_date"),
            "customer_id", "city", "state", "signup_date", "loyalty_tier", "preferred_category",
            F.greatest(F.lit(0), F.datediff(snapshot_reference, F.col("signup_date"))).cast("long").alias("customer_tenure_days"),
            F.coalesce(F.col("completed_orders"), F.lit(0)).cast("long").alias("completed_orders"),
            F.coalesce(F.col("units_purchased"), F.lit(0)).cast("long").alias("units_purchased"),
            F.coalesce(F.col("gross_revenue"), F.lit(0.0)).cast("double").alias("gross_revenue"),
            "first_purchase_timestamp", "last_purchase_timestamp",
            F.when(
                F.col("last_purchase_timestamp").isNotNull(),
                F.greatest(F.lit(0), F.datediff(snapshot_reference, F.to_date("last_purchase_timestamp"))),
            ).cast("long").alias("days_since_last_purchase"),
            "favorite_category", "favorite_brand", "most_recent_category", "most_recent_brand",
            F.coalesce(F.col("payment_attempts"), F.lit(0)).cast("long").alias("payment_attempts"),
            F.coalesce(F.col("captured_payments"), F.lit(0)).cast("long").alias("captured_payments"),
            F.coalesce(F.col("failed_payments"), F.lit(0)).cast("long").alias("failed_payments"),
            F.coalesce(F.col("refunded_payments"), F.lit(0)).cast("long").alias("refunded_payments"),
            F.coalesce(F.col("captured_amount"), F.lit(0.0)).cast("double").alias("captured_amount"),
            F.coalesce(F.col("refunded_amount"), F.lit(0.0)).cast("double").alias("refunded_amount"),
        )
        .withColumn(
            "average_order_value",
            F.when(F.col("completed_orders") > 0, F.col("gross_revenue") / F.col("completed_orders")).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "payment_capture_rate",
            F.when(F.col("payment_attempts") > 0, F.col("captured_payments") / F.col("payment_attempts")).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "customer_status",
            F.when(F.col("last_purchase_timestamp").isNull(), F.lit("Never Purchased"))
            .when(F.col("days_since_last_purchase") <= 30, F.lit("Active"))
            .when(F.col("days_since_last_purchase") <= 90, F.lit("At Risk"))
            .otherwise(F.lit("Inactive")),
        )
        .withColumn("processed_timestamp", F.current_timestamp())
    )


def create_gold_table(spark, gold_full_name):
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {gold_full_name} (
            snapshot_date DATE, customer_id STRING, city STRING, state STRING,
            signup_date DATE, loyalty_tier STRING, preferred_category STRING,
            customer_tenure_days BIGINT, completed_orders BIGINT, units_purchased BIGINT,
            gross_revenue DOUBLE, average_order_value DOUBLE,
            first_purchase_timestamp TIMESTAMP, last_purchase_timestamp TIMESTAMP,
            days_since_last_purchase BIGINT, favorite_category STRING, favorite_brand STRING,
            most_recent_category STRING, most_recent_brand STRING,
            payment_attempts BIGINT, captured_payments BIGINT, failed_payments BIGINT,
            refunded_payments BIGINT, captured_amount DOUBLE, refunded_amount DOUBLE,
            payment_capture_rate DOUBLE, customer_status STRING,
            processed_timestamp TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (snapshot_date)
    """)


def main():
    args = parse_arguments()
    spark = SparkSession.builder.appName("gold_customer_metrics").getOrCreate()
    gold_full_name = f"{args.catalog_name}.{args.gold_database}.{args.gold_table}"

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {args.catalog_name}.{args.gold_database}")
    create_gold_table(spark, gold_full_name)
    customer_metrics = build_customer_metrics(
        spark, args.catalog_name, args.silver_database, args.snapshot_date
    )
    output_count = customer_metrics.count()
    logger.info("Customer Metrics output rows for snapshot %s: %s", args.snapshot_date, output_count)

    # Predicate overwrite replaces just this snapshot date, including an empty
    # current-customer result, without disturbing historical Iceberg snapshots.
    customer_metrics.writeTo(gold_full_name).overwrite(
        F.col("snapshot_date") == F.lit(args.snapshot_date).cast("date")
    )
    logger.info("Wrote Customer Metrics snapshot %s to %s", args.snapshot_date, gold_full_name)
    spark.stop()


if __name__ == "__main__":
    main()
