import sys
from datetime import datetime

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window, functions as F
from pyspark.sql.types import StringType, StructField, StructType


args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "bronze_path", "catalog_name", "silver_database", "silver_table"],
)
sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)
logger = glue_context.get_logger()

catalog_name = args["catalog_name"]
bronze_path = args["bronze_path"]
silver_database = args["silver_database"]
silver_table = args["silver_table"]
silver_full_name = f"{catalog_name}.{silver_database}.{silver_table}"

# Read the known Bronze CSV contract directly; strings allow invalid records to
# be counted before casting instead of relying on inferred or crawler metadata.
bronze_schema = StructType([
    StructField("source_event_id", StringType(), True),
    StructField("payment_id", StringType(), True),
    StructField("operation", StringType(), True),
    StructField("source_updated_at", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("payment_timestamp", StringType(), True),
    StructField("ingested_at", StringType(), True),
    StructField("run_id", StringType(), True),
])

logger.info(f"Reading only supplied Bronze path: {bronze_path}")
bronze_df = spark.read.option("header", "true").schema(bronze_schema).csv(bronze_path)
typed_df = (
    bronze_df.select(*[F.trim(F.col(name)).alias(name) for name in bronze_schema.fieldNames()])
    .withColumn("operation", F.upper(F.col("operation")))
    .withColumn("payment_status", F.upper(F.col("payment_status")))
    .withColumn("currency", F.upper(F.col("currency")))
    .withColumn("amount", F.col("amount").cast("double"))
    .withColumn("payment_timestamp", F.to_timestamp("payment_timestamp"))
    .withColumn("source_updated_at", F.to_timestamp("source_updated_at"))
    .withColumn("ingested_at", F.to_timestamp("ingested_at"))
)

common_valid = (
    F.col("source_event_id").isNotNull() & F.col("payment_id").isNotNull()
    & F.col("operation").isin("I", "U", "D") & F.col("source_updated_at").isNotNull()
    & F.col("ingested_at").isNotNull() & F.col("run_id").isNotNull()
)
state_valid = (
    F.col("order_id").isNotNull() & F.col("payment_method").isNotNull()
    & F.col("payment_status").isin("AUTHORIZED", "CAPTURED", "FAILED", "REFUNDED")
    & F.col("amount").isNotNull() & (F.col("amount") >= 0)
    & (F.col("currency") == "INR") & F.col("payment_timestamp").isNotNull()
)
# Deletes intentionally require no payment business attributes.
validated_df = typed_df.withColumn(
    "_is_valid", common_valid & ((F.col("operation") == "D") | state_valid)
).cache()
total_count = validated_df.count()
valid_count = validated_df.filter("_is_valid").count()
logger.info(f"Payments validation: total={total_count} valid={valid_count} invalid={total_count - valid_count}")

# source_event_id provides deterministic ordering for equal source timestamps.
latest_per_payment = (
    validated_df.filter("_is_valid")
    .withColumn("_row_number", F.row_number().over(
        Window.partitionBy("payment_id").orderBy(
            F.col("source_updated_at").desc(), F.col("source_event_id").desc()
        )
    ))
    .filter(F.col("_row_number") == 1)
    .drop("_row_number", "_is_valid")
    .withColumn("processed_timestamp", F.current_timestamp())
    .cache()
)
logger.info(f"Payments latest CDC events after deduplication: {latest_per_payment.count()}")

spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog_name}.{silver_database}")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {silver_full_name} (
        payment_id STRING, order_id STRING, payment_method STRING,
        payment_status STRING, amount DOUBLE, currency STRING,
        payment_timestamp TIMESTAMP, source_event_id STRING,
        source_updated_at TIMESTAMP, ingested_at TIMESTAMP, run_id STRING,
        processed_timestamp TIMESTAMP
    ) USING iceberg
""")
latest_per_payment.createOrReplaceTempView("payments_cdc_batch")

# Ordering predicates keep an older event from replacing newer payment state.
# An exact replay is logically idempotent, and a replayed delete sees no match.
spark.sql(f"""
    MERGE INTO {silver_full_name} AS target
    USING payments_cdc_batch AS source
    ON target.payment_id = source.payment_id
    WHEN MATCHED AND source.operation = 'D'
      AND (source.source_updated_at > target.source_updated_at
           OR (source.source_updated_at = target.source_updated_at
               AND source.source_event_id >= target.source_event_id)) THEN DELETE
    WHEN MATCHED AND source.operation IN ('I', 'U')
      AND (source.source_updated_at > target.source_updated_at
           OR (source.source_updated_at = target.source_updated_at
               AND source.source_event_id >= target.source_event_id)) THEN UPDATE SET
        order_id = source.order_id,
        payment_method = source.payment_method,
        payment_status = source.payment_status,
        amount = source.amount,
        currency = source.currency,
        payment_timestamp = source.payment_timestamp,
        source_event_id = source.source_event_id,
        source_updated_at = source.source_updated_at,
        ingested_at = source.ingested_at,
        run_id = source.run_id,
        processed_timestamp = source.processed_timestamp
    WHEN NOT MATCHED AND source.operation IN ('I', 'U') THEN INSERT (
        payment_id, order_id, payment_method, payment_status, amount, currency,
        payment_timestamp, source_event_id, source_updated_at, ingested_at,
        run_id, processed_timestamp
    ) VALUES (
        source.payment_id, source.order_id, source.payment_method,
        source.payment_status, source.amount, source.currency,
        source.payment_timestamp, source.source_event_id, source.source_updated_at,
        source.ingested_at, source.run_id, source.processed_timestamp
    )
""")

latest_per_payment.unpersist()
validated_df.unpersist()
job.commit()
logger.info(f"Job {args['JOB_NAME']} finished at {datetime.utcnow().isoformat()}")
