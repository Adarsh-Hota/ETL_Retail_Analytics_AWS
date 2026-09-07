import argparse
import logging
import re
from datetime import date

from pyspark.sql import SparkSession, functions as F


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("gold_product_performance")


def snapshot_date_argument(value):
    """Validate the ISO date used as the immutable business snapshot key."""
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
    parser = argparse.ArgumentParser(description="Build the Product Performance Gold snapshot")
    parser.add_argument("--catalog_name", required=True)
    parser.add_argument("--silver_database", required=True)
    parser.add_argument("--gold_database", required=True)
    parser.add_argument("--gold_table", required=True)
    parser.add_argument("--snapshot_date", required=True, type=snapshot_date_argument)
    return parser.parse_args()


def build_product_performance(spark, catalog_name, silver_database, snapshot_date):
    """Aggregate every Silver source independently before joining product keys."""
    products_table = f"{catalog_name}.{silver_database}.silver_products"
    orders_table = f"{catalog_name}.{silver_database}.silver_orders"
    inventory_table = f"{catalog_name}.{silver_database}.silver_inventory"
    clickstream_table = f"{catalog_name}.{silver_database}.silver_clickstream"

    products_df = spark.table(products_table)
    orders_df = spark.table(orders_table)
    inventory_df = spark.table(inventory_table)
    clickstream_df = spark.table(clickstream_table)
    logger.info(
        "Source row counts: products=%s orders=%s inventory=%s clickstream=%s",
        products_df.count(), orders_df.count(), inventory_df.count(), clickstream_df.count(),
    )

    current_products = products_df.filter(F.col("is_current") == F.lit(True)).select(
        "product_id", "product_name", "category", "subcategory", "brand",
        "is_active", "cost_price", F.col("price").alias("list_price"),
    )

    normalized_orders = orders_df.withColumn(
        "_order_status", F.upper(F.trim(F.col("order_status")))
    )
    # The current simulator emits Delivered, while Completed remains accepted for
    # future sources. Both represent a completed sale in this mart.
    sales_metrics = (
        normalized_orders.filter(F.col("_order_status").isin("COMPLETED", "DELIVERED"))
        .groupBy("product_id")
        .agg(
            F.count("order_id").alias("completed_orders"),
            F.sum("quantity").alias("units_sold"),
            F.sum("total_amount").cast("double").alias("gross_revenue"),
        )
    )
    cancelled_metrics = (
        normalized_orders.filter(F.col("_order_status") == "CANCELLED")
        .groupBy("product_id")
        .agg(F.count("order_id").alias("cancelled_orders"))
    )

    normalized_inventory = inventory_df.withColumn(
        "_movement_type", F.upper(F.trim(F.col("movement_type")))
    )
    inventory_metrics = normalized_inventory.groupBy("product_id").agg(
        F.sum(F.when(F.col("_movement_type") == "STOCK_IN", F.col("quantity_change")).otherwise(F.lit(0))).alias("units_restocked"),
        # Simulator SALE events use negative quantity_change; report sold units
        # as a positive magnitude while retaining the signed net change below.
        F.sum(F.when(F.col("_movement_type") == "SALE", F.abs(F.col("quantity_change"))).otherwise(F.lit(0))).alias("units_sold_from_inventory_events"),
        F.sum(F.col("quantity_change")).alias("net_inventory_change"),
    )

    product_context_clickstream = clickstream_df.filter(F.col("product_id").isNotNull()).withColumn(
        "_event_type", F.lower(F.trim(F.col("event_type")))
    )
    engagement_metrics = product_context_clickstream.groupBy("product_id").agg(
        F.sum(F.when(F.col("_event_type") == "view_product", F.lit(1)).otherwise(F.lit(0))).alias("product_views"),
        F.sum(F.when(F.col("_event_type") == "add_to_cart", F.lit(1)).otherwise(F.lit(0))).alias("add_to_cart_events"),
        F.sum(F.when(F.col("_event_type") == "purchase", F.lit(1)).otherwise(F.lit(0))).alias("purchase_events"),
    )

    metrics = (
        current_products
        .join(sales_metrics, "product_id", "left")
        .join(cancelled_metrics, "product_id", "left")
        .join(inventory_metrics, "product_id", "left")
        .join(engagement_metrics, "product_id", "left")
        .select(
            F.lit(snapshot_date).cast("date").alias("snapshot_date"),
            "product_id", "product_name", "category", "subcategory", "brand", "is_active",
            F.col("cost_price").cast("double").alias("cost_price"),
            F.col("list_price").cast("double").alias("list_price"),
            F.coalesce(F.col("completed_orders"), F.lit(0)).cast("long").alias("completed_orders"),
            F.coalesce(F.col("units_sold"), F.lit(0)).cast("long").alias("units_sold"),
            F.coalesce(F.col("gross_revenue"), F.lit(0.0)).cast("double").alias("gross_revenue"),
            F.coalesce(F.col("cancelled_orders"), F.lit(0)).cast("long").alias("cancelled_orders"),
            F.coalesce(F.col("product_views"), F.lit(0)).cast("long").alias("product_views"),
            F.coalesce(F.col("add_to_cart_events"), F.lit(0)).cast("long").alias("add_to_cart_events"),
            F.coalesce(F.col("purchase_events"), F.lit(0)).cast("long").alias("purchase_events"),
            F.coalesce(F.col("units_restocked"), F.lit(0)).cast("long").alias("units_restocked"),
            F.coalesce(F.col("units_sold_from_inventory_events"), F.lit(0)).cast("long").alias("units_sold_from_inventory_events"),
            F.coalesce(F.col("net_inventory_change"), F.lit(0)).cast("long").alias("net_inventory_change"),
        )
        .withColumn(
            "average_selling_price",
            F.when(F.col("units_sold") > 0, F.col("gross_revenue") / F.col("units_sold")).otherwise(F.lit(0.0)),
        )
        .withColumn("estimated_cost_of_goods_sold", F.col("units_sold") * F.col("cost_price"))
        .withColumn("estimated_gross_profit", F.col("gross_revenue") - F.col("estimated_cost_of_goods_sold"))
        .withColumn(
            "gross_margin_pct",
            F.when(F.col("gross_revenue") > 0, F.col("estimated_gross_profit") / F.col("gross_revenue")).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "view_to_cart_rate",
            F.when(F.col("product_views") > 0, F.col("add_to_cart_events") / F.col("product_views")).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "view_to_purchase_rate",
            F.when(F.col("product_views") > 0, F.col("purchase_events") / F.col("product_views")).otherwise(F.lit(0.0)),
        )
        .withColumn("processed_timestamp", F.current_timestamp())
    )
    return metrics


def create_gold_table(spark, gold_full_name):
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {gold_full_name} (
            snapshot_date DATE, product_id STRING, product_name STRING,
            category STRING, subcategory STRING, brand STRING, is_active BOOLEAN,
            cost_price DOUBLE, list_price DOUBLE, completed_orders BIGINT,
            units_sold BIGINT, gross_revenue DOUBLE, average_selling_price DOUBLE,
            estimated_cost_of_goods_sold DOUBLE, estimated_gross_profit DOUBLE,
            gross_margin_pct DOUBLE, cancelled_orders BIGINT, product_views BIGINT,
            add_to_cart_events BIGINT, purchase_events BIGINT, view_to_cart_rate DOUBLE,
            view_to_purchase_rate DOUBLE, units_restocked BIGINT,
            units_sold_from_inventory_events BIGINT, net_inventory_change BIGINT,
            processed_timestamp TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (snapshot_date)
    """)


def main():
    args = parse_arguments()
    spark = SparkSession.builder.appName("gold_product_performance").getOrCreate()
    gold_full_name = f"{args.catalog_name}.{args.gold_database}.{args.gold_table}"

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {args.catalog_name}.{args.gold_database}")
    create_gold_table(spark, gold_full_name)
    product_performance = build_product_performance(
        spark, args.catalog_name, args.silver_database, args.snapshot_date
    )
    output_count = product_performance.count()
    logger.info("Product Performance output row count for snapshot %s: %s", args.snapshot_date, output_count)

    # Iceberg replaces only this date predicate (including when the result is
    # empty), retaining every other snapshot_date partition atomically.
    product_performance.writeTo(gold_full_name).overwrite(
        F.col("snapshot_date") == F.lit(args.snapshot_date).cast("date")
    )
    logger.info("Wrote Product Performance snapshot %s to %s", args.snapshot_date, gold_full_name)
    spark.stop()


if __name__ == "__main__":
    main()
