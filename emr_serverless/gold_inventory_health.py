"""Build an Iceberg Inventory Health snapshot from current Silver tables.

Limitations retained by design:
* On-hand units are the net of available Inventory event history; there is no
  opening-balance or physical-stock reconciliation source.
* Product attributes are from the current Product SCD2 record, not Product state
  as of ``snapshot_date``.
* Late Inventory events for an already-published date appear only if that snapshot
  date is run again.
"""

import argparse
import logging
import re
from datetime import date

from pyspark.sql import DataFrame, SparkSession, functions as F


LOGGER = logging.getLogger(__name__)
DATE_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}")


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


def non_negative_int(value: str) -> int:
    """Parse a non-negative integer without accepting signs, fractions, or blanks."""
    if not re.fullmatch(r"\d+", value):
        raise argparse.ArgumentTypeError("must be a non-negative integer")
    return int(value)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build an Iceberg Inventory Health snapshot")
    parser.add_argument("--catalog_name", required=True)
    parser.add_argument("--silver_database", required=True)
    parser.add_argument("--gold_database", required=True)
    parser.add_argument("--gold_table", required=True)
    parser.add_argument("--snapshot_date", required=True, type=valid_iso_date)
    parser.add_argument("--low_stock_threshold", required=True, type=non_negative_int)
    return parser.parse_args()


def build_inventory_health(
    inventory: DataFrame,
    products: DataFrame,
    snapshot_date: str,
    low_stock_threshold: int,
) -> DataFrame:
    """Aggregate inventory events through the supplied business-date cutoff."""
    current_products = (
        products.filter(F.col("is_current") == F.lit(True))
        .select("product_id", "product_name", "category", "subcategory", "brand", "is_active")
        .dropDuplicates(["product_id"])
    )

    eligible_events = (
        inventory.withColumn("event_date", F.to_date(F.col("event_timestamp")))
        .filter(F.col("event_date") <= F.lit(snapshot_date).cast("date"))
        .withColumn("normalized_movement_type", F.upper(F.trim(F.col("movement_type"))))
        .withColumn("quantity_change", F.col("quantity_change").cast("long"))
    )
    quantity = F.col("quantity_change")
    movement = F.col("normalized_movement_type")

    inventory_metrics = eligible_events.groupBy("product_id", "warehouse_id").agg(
        F.sum(quantity).cast("long").alias("on_hand_units"),
        F.sum(
            F.when((movement == "STOCK_IN") & (quantity > F.lit(0)), quantity).otherwise(F.lit(0))
        )
        .cast("long")
        .alias("stock_in_units"),
        F.sum(F.when(movement == "STOCK_OUT", F.abs(quantity)).otherwise(F.lit(0)))
        .cast("long")
        .alias("stock_out_units"),
        F.sum(F.when(movement == "SALE", F.abs(quantity)).otherwise(F.lit(0)))
        .cast("long")
        .alias("sale_units"),
        F.sum(F.when(movement == "ADJUSTMENT", quantity).otherwise(F.lit(0)))
        .cast("long")
        .alias("adjustment_units"),
        F.count(F.lit(1)).cast("long").alias("inventory_event_count"),
        F.max(F.col("event_timestamp")).alias("last_inventory_event_timestamp"),
    )

    threshold = F.lit(low_stock_threshold).cast("long")
    return (
        inventory_metrics.join(current_products, on="product_id", how="left")
        .withColumn("snapshot_date", F.lit(snapshot_date).cast("date"))
        .withColumn("category", F.coalesce(F.col("category"), F.lit("UNKNOWN")))
        .withColumn("low_stock_threshold", threshold)
        .withColumn(
            "stock_status",
            F.when(F.col("on_hand_units") <= F.lit(0), F.lit("OUT_OF_STOCK"))
            .when(F.col("on_hand_units") <= threshold, F.lit("LOW_STOCK"))
            .otherwise(F.lit("IN_STOCK")),
        )
        .withColumn("processed_timestamp", F.current_timestamp())
        .select(
            "snapshot_date",
            "product_id",
            "warehouse_id",
            "product_name",
            "category",
            "subcategory",
            "brand",
            "is_active",
            "on_hand_units",
            "stock_in_units",
            "stock_out_units",
            "sale_units",
            "adjustment_units",
            "inventory_event_count",
            "last_inventory_event_timestamp",
            "low_stock_threshold",
            "stock_status",
            "processed_timestamp",
        )
    )


def create_target_table(spark: SparkSession, table_name: str) -> None:
    """Create the Glue Catalog-backed Iceberg target when it does not yet exist."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {table_name.rsplit('.', 1)[0]}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            snapshot_date DATE,
            product_id STRING,
            warehouse_id STRING,
            product_name STRING,
            category STRING,
            subcategory STRING,
            brand STRING,
            is_active BOOLEAN,
            on_hand_units BIGINT,
            stock_in_units BIGINT,
            stock_out_units BIGINT,
            sale_units BIGINT,
            adjustment_units BIGINT,
            inventory_event_count BIGINT,
            last_inventory_event_timestamp TIMESTAMP,
            low_stock_threshold BIGINT,
            stock_status STRING,
            processed_timestamp TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (snapshot_date)
        """
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    spark = SparkSession.builder.appName("gold_inventory_health").getOrCreate()

    try:
        inventory_table = f"{args.catalog_name}.{args.silver_database}.silver_inventory"
        products_table = f"{args.catalog_name}.{args.silver_database}.silver_products"
        gold_table = f"{args.catalog_name}.{args.gold_database}.{args.gold_table}"

        inventory = spark.table(inventory_table)
        products = spark.table(products_table)
        LOGGER.info("Source count: inventory=%s", inventory.count())
        LOGGER.info("Source count: products=%s", products.count())

        inventory_health = build_inventory_health(
            inventory, products, args.snapshot_date, args.low_stock_threshold
        )
        output_count = inventory_health.count()
        LOGGER.info("Inventory Health output rows for %s: %s", args.snapshot_date, output_count)

        create_target_table(spark, gold_table)
        # Predicate overwrite replaces only this snapshot partition. An empty result
        # clears stale rows for this date without changing historical snapshots.
        inventory_health.writeTo(gold_table).overwrite(
            F.col("snapshot_date") == F.lit(args.snapshot_date).cast("date")
        )
        LOGGER.info("Completed Inventory Health refresh for snapshot_date=%s", args.snapshot_date)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
