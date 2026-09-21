import argparse
import logging
import re
from datetime import date

from pyspark.sql import SparkSession, functions as F


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("gold_conversion_funnel")

KNOWN_DEVICE_TYPES = ("mobile", "desktop", "tablet")
FUNNEL_STAGES = (
    ("PRODUCT_VIEW", 1),
    ("ADD_TO_CART", 2),
    ("CHECKOUT", 3),
    ("PURCHASE", 4),
)


def funnel_date_argument(value):
    """Strictly validate the date partition refreshed by this snapshot job."""
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise argparse.ArgumentTypeError("funnel_date must use YYYY-MM-DD format")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("funnel_date must be a valid ISO date") from error
    if parsed.isoformat() != value:
        raise argparse.ArgumentTypeError("funnel_date must use YYYY-MM-DD format")
    return value


def parse_arguments():
    parser = argparse.ArgumentParser(description="Build a daily Conversion Funnel Gold snapshot")
    parser.add_argument("--catalog_name", required=True)
    parser.add_argument("--gold_database", required=True)
    parser.add_argument("--session_table", required=True)
    parser.add_argument("--gold_table", required=True)
    parser.add_argument("--funnel_date", required=True, type=funnel_date_argument)
    return parser.parse_args()


def build_conversion_funnel(spark, session_table_full_name, funnel_date):
    """Aggregate session-level stage eligibility by date and normalized device."""
    sessions = spark.table(session_table_full_name)
    logger.info("Source session count: %s", sessions.count())

    sessions_for_date = sessions.filter(
        F.col("session_date") == F.lit(funnel_date).cast("date")
    )
    logger.info("Sessions matching funnel date %s: %s", funnel_date, sessions_for_date.count())

    normalized_sessions = (
        sessions_for_date
        .withColumn("device_type", F.lower(F.trim(F.col("device_type"))))
        # Silver validates these values; retain only its known device contract.
        .filter(F.col("device_type").isin(*KNOWN_DEVICE_TYPES))
    )

    device_metrics = normalized_sessions.groupBy("device_type").agg(
        F.count("session_id").alias("total_sessions"),
        F.sum(F.when(F.col("product_view_events") > 0, F.lit(1)).otherwise(F.lit(0))).alias("product_view_sessions"),
        F.sum(F.when(F.col("has_add_to_cart"), F.lit(1)).otherwise(F.lit(0))).alias("add_to_cart_sessions"),
        F.sum(F.when(F.col("has_checkout"), F.lit(1)).otherwise(F.lit(0))).alias("checkout_sessions"),
        F.sum(F.when(F.col("has_purchase"), F.lit(1)).otherwise(F.lit(0))).alias("purchase_sessions"),
    )
    logger.info("Device groups for funnel date %s: %s", funnel_date, device_metrics.count())

    stage_dimension = spark.createDataFrame(FUNNEL_STAGES, ["funnel_stage", "_stage_order"])
    funnel_rows = device_metrics.crossJoin(stage_dimension).withColumn(
        "sessions_reaching_stage",
        F.when(F.col("funnel_stage") == "PRODUCT_VIEW", F.col("product_view_sessions"))
        .when(F.col("funnel_stage") == "ADD_TO_CART", F.col("add_to_cart_sessions"))
        .when(F.col("funnel_stage") == "CHECKOUT", F.col("checkout_sessions"))
        .otherwise(F.col("purchase_sessions")),
    ).withColumn(
        "previous_stage_sessions",
        F.when(F.col("funnel_stage") == "PRODUCT_VIEW", F.col("total_sessions"))
        .when(F.col("funnel_stage") == "ADD_TO_CART", F.col("product_view_sessions"))
        .when(F.col("funnel_stage") == "CHECKOUT", F.col("add_to_cart_sessions"))
        .otherwise(F.col("checkout_sessions")),
    )

    return funnel_rows.select(
        F.lit(funnel_date).cast("date").alias("funnel_date"),
        "device_type", "funnel_stage",
        F.col("total_sessions").cast("long").alias("total_sessions"),
        F.col("sessions_reaching_stage").cast("long").alias("sessions_reaching_stage"),
        F.col("previous_stage_sessions").cast("long").alias("previous_stage_sessions"),
        F.when(
            F.col("previous_stage_sessions") > 0,
            F.col("sessions_reaching_stage") / F.col("previous_stage_sessions"),
        ).otherwise(F.lit(0.0)).alias("stage_conversion_rate"),
        F.greatest(
            F.lit(0), F.col("previous_stage_sessions") - F.col("sessions_reaching_stage")
        ).cast("long").alias("stage_dropoff_sessions"),
        F.when(
            F.col("total_sessions") > 0,
            F.col("purchase_sessions") / F.col("total_sessions"),
        ).otherwise(F.lit(0.0)).alias("overall_purchase_conversion_rate"),
        F.current_timestamp().alias("processed_timestamp"),
    )


def create_gold_table(spark, gold_full_name):
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {gold_full_name} (
            funnel_date DATE, device_type STRING, funnel_stage STRING,
            total_sessions BIGINT, sessions_reaching_stage BIGINT,
            previous_stage_sessions BIGINT, stage_conversion_rate DOUBLE,
            stage_dropoff_sessions BIGINT, overall_purchase_conversion_rate DOUBLE,
            processed_timestamp TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (funnel_date)
    """)


def main():
    args = parse_arguments()
    spark = SparkSession.builder.appName("gold_conversion_funnel").getOrCreate()
    session_table_full_name = f"{args.catalog_name}.{args.gold_database}.{args.session_table}"
    gold_full_name = f"{args.catalog_name}.{args.gold_database}.{args.gold_table}"

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {args.catalog_name}.{args.gold_database}")
    create_gold_table(spark, gold_full_name)
    funnel = build_conversion_funnel(spark, session_table_full_name, args.funnel_date)
    output_count = funnel.count()
    logger.info("Conversion Funnel output rows for %s: %s", args.funnel_date, output_count)

    # Predicate overwrite replaces only this funnel_date, including with an
    # empty source result, while Iceberg retains all other date partitions.
    funnel.writeTo(gold_full_name).overwrite(
        F.col("funnel_date") == F.lit(args.funnel_date).cast("date")
    )
    logger.info("Wrote Conversion Funnel date %s to %s", args.funnel_date, gold_full_name)
    spark.stop()


if __name__ == "__main__":
    main()
