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

# Firehose is expected to land one JSON event per line. The schema remains
# explicit to avoid accepting unexpected fields or inferred types.
bronze_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("ingested_at", StringType(), True),
    StructField("run_id", StringType(), True),
])

logger.info(f"Reading only supplied Bronze path: {bronze_path}")
bronze_df = spark.read.schema(bronze_schema).json(bronze_path)
typed_df = (
    bronze_df.select(*[F.trim(F.col(name)).alias(name) for name in bronze_schema.fieldNames()])
    .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
    .withColumn("ingested_at", F.to_timestamp("ingested_at"))
    .withColumn("event_type", F.lower(F.col("event_type")))
    .withColumn("device_type", F.lower(F.col("device_type")))
)

product_context_events = ["view_product", "add_to_cart", "remove_from_cart", "checkout", "purchase"]
valid_event_types = ["page_view"] + product_context_events
is_valid = (
    F.col("event_id").isNotNull() & F.col("event_timestamp").isNotNull()
    & F.col("session_id").isNotNull() & F.col("event_type").isin(valid_event_types)
    & F.col("page_url").isNotNull() & F.col("device_type").isin("mobile", "desktop", "tablet")
    & F.col("ingested_at").isNotNull() & F.col("run_id").isNotNull()
    & ((~F.col("event_type").isin(product_context_events)) | F.col("product_id").isNotNull())
)
validated_df = typed_df.withColumn("_is_valid", is_valid).cache()
total_count = validated_df.count()
valid_count = validated_df.filter("_is_valid").count()
logger.info(f"Clickstream validation: total={total_count} valid={valid_count} invalid={total_count - valid_count}")

# Stable ordering makes duplicate newline-delimited copies of an event resolve
# to one candidate before target-level replay protection is applied.
batch_events = (
    validated_df.filter("_is_valid")
    .withColumn("_row_number", F.row_number().over(
        Window.partitionBy("event_id").orderBy(
            F.col("event_timestamp").desc(), F.col("ingested_at").desc(), F.col("run_id").desc()
        )
    ))
    .filter(F.col("_row_number") == 1)
    .drop("_row_number", "_is_valid")
    .withColumn("processed_timestamp", F.current_timestamp())
    .cache()
)
logger.info(f"Clickstream events after batch deduplication: {batch_events.count()}")

spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog_name}.{silver_database}")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {silver_full_name} (
        event_id STRING, event_timestamp TIMESTAMP, customer_id STRING,
        session_id STRING, product_id STRING, event_type STRING, page_url STRING,
        device_type STRING, ingested_at TIMESTAMP, run_id STRING,
        processed_timestamp TIMESTAMP
    ) USING iceberg
""")
batch_events.createOrReplaceTempView("clickstream_events_batch")

# Append-only event semantics: target anti-join makes a sequential replay of a
# Firehose-landed Bronze partition unable to append the same logical event ID.
spark.sql(f"""
    INSERT INTO {silver_full_name}
    SELECT source.event_id, source.event_timestamp, source.customer_id,
           source.session_id, source.product_id, source.event_type, source.page_url,
           source.device_type, source.ingested_at, source.run_id,
           source.processed_timestamp
    FROM clickstream_events_batch AS source
    LEFT ANTI JOIN {silver_full_name} AS target
      ON source.event_id = target.event_id
""")

batch_events.unpersist()
validated_df.unpersist()
job.commit()
logger.info(f"Job {args['JOB_NAME']} finished at {datetime.utcnow().isoformat()}")
