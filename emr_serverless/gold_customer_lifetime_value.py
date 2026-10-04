"""Build a deterministic observed Customer Lifetime Value Iceberg snapshot.

This is observed value from completed Order revenue, not a predictive CLV model.
Customers, Orders, and Payments are read in their currently available Silver state;
the job does not reconstruct their historical state as of ``snapshot_date``. Payment
records are latest state, so payment outcomes are reported separately and are not
used to claim a fully payment-adjusted net-revenue calculation. Late source events
for a published date appear only when that snapshot date is run again.
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
    parser = argparse.ArgumentParser(description="Build an observed Customer Lifetime Value snapshot")
    parser.add_argument("--catalog_name", required=True)
    parser.add_argument("--silver_database", required=True)
    parser.add_argument("--gold_database", required=True)
    parser.add_argument("--gold_table", required=True)
    parser.add_argument("--snapshot_date", required=True, type=valid_iso_date)
    return parser.parse_args()


def build_customer_lifetime_value(
    customers: DataFrame,
    orders: DataFrame,
    payments: DataFrame,
    snapshot_date: str,
) -> DataFrame:
    """Create customer-level metrics without allowing payment rows to multiply orders."""
    snapshot_reference = F.lit(snapshot_date).cast("date")
    # Deliberately omit first_name, last_name, and email from every Gold path.
    current_customers = customers.filter(F.col("is_current") == F.lit(True)).select(
        "customer_id", "city", "state", "signup_date", "loyalty_tier", "preferred_category"
    )

    eligible_orders = (
        orders.withColumn("order_date", F.to_date(F.col("order_timestamp")))
        .filter(F.col("order_date") <= snapshot_reference)
        .withColumn("normalized_order_status", F.upper(F.trim(F.col("order_status"))))
        .select(
            "order_id",
            "customer_id",
            "quantity",
            "total_amount",
            "order_timestamp",
            "normalized_order_status",
        )
    )
    completed_orders = eligible_orders.filter(
        F.col("normalized_order_status").isin(*COMPLETED_ORDER_STATUSES)
    )
    order_metrics = completed_orders.groupBy("customer_id").agg(
        F.count(F.lit(1)).cast("long").alias("lifetime_completed_orders"),
        F.sum(F.col("quantity").cast("long")).cast("long").alias("lifetime_units_purchased"),
        F.sum(F.col("total_amount").cast("double")).cast("double").alias(
            "lifetime_gross_revenue"
        ),
        F.min(F.col("order_timestamp")).alias("first_purchase_timestamp"),
        F.max(F.col("order_timestamp")).alias("last_purchase_timestamp"),
    )

    # Map only payments belonging to eligible Orders, then aggregate them before
    # joining Customers. Multiple attempts therefore cannot multiply order metrics.
    eligible_order_customers = eligible_orders.select("order_id", "customer_id").dropDuplicates(
        ["order_id"]
    )
    eligible_payments = (
        payments.withColumn("payment_date", F.to_date(F.col("payment_timestamp")))
        .filter(F.col("payment_date") <= snapshot_reference)
        .withColumn("normalized_payment_status", F.upper(F.trim(F.col("payment_status"))))
        .join(eligible_order_customers, on="order_id", how="inner")
    )
    payment_metrics = eligible_payments.groupBy("customer_id").agg(
        F.count(F.lit(1)).cast("long").alias("payment_attempts"),
        F.sum(
            F.when(F.col("normalized_payment_status") == "CAPTURED", F.lit(1)).otherwise(F.lit(0))
        )
        .cast("long")
        .alias("captured_payments"),
        F.sum(
            F.when(F.col("normalized_payment_status") == "FAILED", F.lit(1)).otherwise(F.lit(0))
        )
        .cast("long")
        .alias("failed_payments"),
        F.sum(
            F.when(F.col("normalized_payment_status") == "REFUNDED", F.lit(1)).otherwise(F.lit(0))
        )
        .cast("long")
        .alias("refunded_payments"),
        F.sum(
            F.when(
                F.col("normalized_payment_status") == "CAPTURED", F.col("amount").cast("double")
            ).otherwise(F.lit(0.0))
        )
        .cast("double")
        .alias("lifetime_captured_amount"),
        F.sum(
            F.when(
                F.col("normalized_payment_status") == "REFUNDED", F.col("amount").cast("double")
            ).otherwise(F.lit(0.0))
        )
        .cast("double")
        .alias("lifetime_refunded_amount"),
    )

    zero_long = F.lit(0).cast("long")
    zero_double = F.lit(0.0).cast("double")
    combined = current_customers.join(order_metrics, on="customer_id", how="left").join(
        payment_metrics, on="customer_id", how="left"
    )
    base = combined.select(
        snapshot_reference.alias("snapshot_date"),
        "customer_id",
        "city",
        "state",
        "signup_date",
        "loyalty_tier",
        "preferred_category",
        F.greatest(F.lit(0), F.datediff(snapshot_reference, F.col("signup_date")))
        .cast("long")
        .alias("customer_tenure_days"),
        F.coalesce(F.col("lifetime_completed_orders"), zero_long).alias(
            "lifetime_completed_orders"
        ),
        F.coalesce(F.col("lifetime_units_purchased"), zero_long).alias(
            "lifetime_units_purchased"
        ),
        F.coalesce(F.col("lifetime_gross_revenue"), zero_double).alias(
            "lifetime_gross_revenue"
        ),
        "first_purchase_timestamp",
        "last_purchase_timestamp",
        F.when(
            F.col("last_purchase_timestamp").isNotNull(),
            F.greatest(
                F.lit(0), F.datediff(snapshot_reference, F.to_date("last_purchase_timestamp"))
            ),
        )
        .cast("long")
        .alias("days_since_last_purchase"),
        F.coalesce(F.col("payment_attempts"), zero_long).alias("payment_attempts"),
        F.coalesce(F.col("captured_payments"), zero_long).alias("captured_payments"),
        F.coalesce(F.col("failed_payments"), zero_long).alias("failed_payments"),
        F.coalesce(F.col("refunded_payments"), zero_long).alias("refunded_payments"),
        F.coalesce(F.col("lifetime_captured_amount"), zero_double).alias(
            "lifetime_captured_amount"
        ),
        F.coalesce(F.col("lifetime_refunded_amount"), zero_double).alias(
            "lifetime_refunded_amount"
        ),
    )

    return (
        base.withColumn(
            "average_order_value",
            F.when(
                F.col("lifetime_completed_orders") > F.lit(0),
                F.col("lifetime_gross_revenue") / F.col("lifetime_completed_orders"),
            ).otherwise(zero_double),
        )
        .withColumn(
            "purchase_frequency_per_30_days",
            F.when(
                F.col("lifetime_completed_orders") > F.lit(0),
                F.col("lifetime_completed_orders").cast("double")
                / F.greatest(
                    F.lit(1.0), F.col("customer_tenure_days").cast("double") / F.lit(30.0)
                ),
            ).otherwise(zero_double),
        )
        .withColumn("observed_lifetime_value", F.col("lifetime_gross_revenue").cast("double"))
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
    """Create the parameterized Glue Catalog Iceberg target when absent."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {table_name.rsplit('.', 1)[0]}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            snapshot_date DATE,
            customer_id STRING,
            city STRING,
            state STRING,
            signup_date DATE,
            loyalty_tier STRING,
            preferred_category STRING,
            customer_tenure_days BIGINT,
            lifetime_completed_orders BIGINT,
            lifetime_units_purchased BIGINT,
            lifetime_gross_revenue DOUBLE,
            first_purchase_timestamp TIMESTAMP,
            last_purchase_timestamp TIMESTAMP,
            average_order_value DOUBLE,
            days_since_last_purchase BIGINT,
            purchase_frequency_per_30_days DOUBLE,
            observed_lifetime_value DOUBLE,
            payment_attempts BIGINT,
            captured_payments BIGINT,
            failed_payments BIGINT,
            refunded_payments BIGINT,
            lifetime_captured_amount DOUBLE,
            lifetime_refunded_amount DOUBLE,
            payment_capture_rate DOUBLE,
            processed_timestamp TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (snapshot_date)
        """
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    spark = SparkSession.builder.appName("gold_customer_lifetime_value").getOrCreate()

    try:
        customers_table = f"{args.catalog_name}.{args.silver_database}.silver_customers"
        orders_table = f"{args.catalog_name}.{args.silver_database}.silver_orders"
        payments_table = f"{args.catalog_name}.{args.silver_database}.silver_payments"
        gold_table = f"{args.catalog_name}.{args.gold_database}.{args.gold_table}"

        customers = spark.table(customers_table)
        orders = spark.table(orders_table)
        payments = spark.table(payments_table)
        LOGGER.info("Source count: customers=%s", customers.count())
        LOGGER.info("Source count: orders=%s", orders.count())
        LOGGER.info("Source count: payments=%s", payments.count())

        customer_lifetime_value = build_customer_lifetime_value(
            customers, orders, payments, args.snapshot_date
        )
        output_count = customer_lifetime_value.count()
        LOGGER.info("Customer Lifetime Value output rows for %s: %s", args.snapshot_date, output_count)

        create_target_table(spark, gold_table)
        # Predicate overwrite replaces only this snapshot partition. An empty current
        # Customer result clears stale rows for this date and retains all prior dates.
        customer_lifetime_value.writeTo(gold_table).overwrite(
            F.col("snapshot_date") == F.lit(args.snapshot_date).cast("date")
        )
        LOGGER.info("Completed Customer Lifetime Value snapshot %s", args.snapshot_date)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
