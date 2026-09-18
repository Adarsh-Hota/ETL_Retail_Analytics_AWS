import argparse
import logging
import re
from datetime import date

from pyspark.sql import SparkSession, Window, functions as F


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("gold_clickstream_session_analytics")


def processing_date_argument(value):
    """Require the date that identifies sessions to publish in this run."""
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise argparse.ArgumentTypeError("processing_date must use YYYY-MM-DD format")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("processing_date must be a valid ISO date") from error
    if parsed.isoformat() != value:
        raise argparse.ArgumentTypeError("processing_date must use YYYY-MM-DD format")
    return value


def parse_arguments():
    parser = argparse.ArgumentParser(description="Build incremental Clickstream Session Analytics Gold rows")
    parser.add_argument("--catalog_name", required=True)
    parser.add_argument("--silver_database", required=True)
    parser.add_argument("--gold_database", required=True)
    parser.add_argument("--gold_table", required=True)
    parser.add_argument("--processing_date", required=True, type=processing_date_argument)
    return parser.parse_args()


def build_sessions(spark, catalog_name, silver_database):
    """Build one deterministic aggregate row for every available session."""
    clickstream_table = f"{catalog_name}.{silver_database}.silver_clickstream"
    clickstream_df = spark.table(clickstream_table)
    logger.info("Input Clickstream row count: %s", clickstream_df.count())

    events = clickstream_df.select(
        "event_id", "event_timestamp", "customer_id", "session_id", "product_id",
        "event_type", "page_url", "device_type", "ingested_at", "run_id",
        "processed_timestamp",
    ).withColumn("_event_type", F.lower(F.trim(F.col("event_type"))))

    # Earliest time and lexicographically smallest event_id make session-level
    # customer/device selection repeatable, including for anonymous sessions.
    first_event_window = Window.partitionBy("session_id").orderBy(
        F.col("event_timestamp").asc(), F.col("event_id").asc()
    )
    first_session_attributes = (
        events.withColumn("_row_number", F.row_number().over(first_event_window))
        .filter(F.col("_row_number") == 1)
        .select("session_id", "customer_id", "device_type")
    )

    session_metrics = events.groupBy("session_id").agg(
        F.min("event_timestamp").alias("session_start_timestamp"),
        F.max("event_timestamp").alias("session_end_timestamp"),
        F.count("event_id").alias("event_count"),
        F.sum(F.when(F.col("_event_type") == "page_view", F.lit(1)).otherwise(F.lit(0))).alias("page_view_events"),
        F.sum(F.when(F.col("_event_type") == "view_product", F.lit(1)).otherwise(F.lit(0))).alias("product_view_events"),
        F.sum(F.when(F.col("_event_type") == "add_to_cart", F.lit(1)).otherwise(F.lit(0))).alias("add_to_cart_events"),
        F.sum(F.when(F.col("_event_type") == "remove_from_cart", F.lit(1)).otherwise(F.lit(0))).alias("remove_from_cart_events"),
        F.sum(F.when(F.col("_event_type") == "checkout", F.lit(1)).otherwise(F.lit(0))).alias("checkout_events"),
        F.sum(F.when(F.col("_event_type") == "purchase", F.lit(1)).otherwise(F.lit(0))).alias("purchase_events"),
        F.countDistinct(F.when(F.col("_event_type") == "view_product", F.col("product_id"))).alias("unique_products_viewed"),
    )

    return (
        first_session_attributes.join(session_metrics, "session_id", "inner")
        .withColumn("session_date", F.to_date(F.col("session_start_timestamp")))
        .withColumn(
            "session_duration_seconds",
            F.greatest(
                F.lit(0),
                F.unix_timestamp("session_end_timestamp") - F.unix_timestamp("session_start_timestamp"),
            ).cast("long"),
        )
        .withColumn("has_add_to_cart", F.col("add_to_cart_events") > 0)
        .withColumn("has_checkout", F.col("checkout_events") > 0)
        .withColumn("has_purchase", F.col("purchase_events") > 0)
        .withColumn(
            "funnel_stage",
            F.when(F.col("purchase_events") > 0, F.lit("PURCHASED"))
            .when(F.col("checkout_events") > 0, F.lit("CHECKOUT"))
            .when(F.col("add_to_cart_events") > 0, F.lit("CART"))
            .when(F.col("product_view_events") > 0, F.lit("PRODUCT_VIEW"))
            .otherwise(F.lit("BROWSE")),
        )
        .withColumn("processed_timestamp", F.current_timestamp())
        .select(
            "session_date", "session_id", "customer_id", "device_type",
            "session_start_timestamp", "session_end_timestamp", "session_duration_seconds",
            "event_count", "page_view_events", "product_view_events", "add_to_cart_events",
            "remove_from_cart_events", "checkout_events", "purchase_events",
            "unique_products_viewed", "has_add_to_cart", "has_checkout", "has_purchase",
            "funnel_stage", "processed_timestamp",
        )
    )


def create_gold_table(spark, gold_full_name):
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {gold_full_name} (
            session_date DATE, session_id STRING, customer_id STRING, device_type STRING,
            session_start_timestamp TIMESTAMP, session_end_timestamp TIMESTAMP,
            session_duration_seconds BIGINT, event_count BIGINT, page_view_events BIGINT,
            product_view_events BIGINT, add_to_cart_events BIGINT,
            remove_from_cart_events BIGINT, checkout_events BIGINT, purchase_events BIGINT,
            unique_products_viewed BIGINT, has_add_to_cart BOOLEAN, has_checkout BOOLEAN,
            has_purchase BOOLEAN, funnel_stage STRING, processed_timestamp TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (session_date)
    """)


def main():
    args = parse_arguments()
    spark = SparkSession.builder.appName("gold_clickstream_session_analytics").getOrCreate()
    gold_full_name = f"{args.catalog_name}.{args.gold_database}.{args.gold_table}"

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {args.catalog_name}.{args.gold_database}")
    create_gold_table(spark, gold_full_name)

    # Aggregate all session events first: sessions crossing midnight retain every
    # event available when their session-start date is selected for publication.
    all_sessions = build_sessions(spark, args.catalog_name, args.silver_database)
    logger.info("Candidate session count: %s", all_sessions.count())
    sessions_for_date = all_sessions.filter(
        F.col("session_date") == F.lit(args.processing_date).cast("date")
    ).cache()
    sessions_for_date_count = sessions_for_date.count()
    logger.info("Sessions for processing date %s: %s", args.processing_date, sessions_for_date_count)

    existing_session_ids = spark.table(gold_full_name).select("session_id").distinct()
    existing_count = sessions_for_date.join(existing_session_ids, "session_id", "inner").count()
    sessions_to_append = sessions_for_date.join(existing_session_ids, "session_id", "left_anti").cache()
    append_count = sessions_to_append.count()
    logger.info("Sessions skipped as existing: %s", existing_count)
    logger.info("Sessions appended: %s", append_count)

    # This is deliberately append-only. A session is immutable once first
    # published; late Clickstream events for an existing session are not merged.
    if append_count:
        sessions_to_append.writeTo(gold_full_name).append()

    sessions_to_append.unpersist()
    sessions_for_date.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
