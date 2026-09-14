import sys
from datetime import datetime

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window, functions as F
from pyspark.sql.types import StructField, StructType, StringType


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

bronze_schema = StructType([
    StructField("inventory_event_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("warehouse_id", StringType(), True),
    StructField("movement_type", StringType(), True),
    StructField("quantity_change", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("ingested_at", StringType(), True),
    StructField("run_id", StringType(), True),
])

logger.info(f"Reading only supplied Bronze path: {bronze_path}")
bronze_df = spark.read.option("header", "true").schema(bronze_schema).csv(bronze_path)
typed_df = (
    bronze_df.select(*[F.trim(F.col(name)).alias(name) for name in bronze_schema.fieldNames()])
    .withColumn("movement_type", F.upper(F.col("movement_type")))
    .withColumn("quantity_change", F.col("quantity_change").cast("int"))
    .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
    .withColumn("ingested_at", F.to_timestamp("ingested_at"))
)
is_valid = (
    F.col("inventory_event_id").isNotNull() & F.col("product_id").isNotNull()
    & F.col("warehouse_id").isNotNull()
    & F.col("movement_type").isin("STOCK_IN", "STOCK_OUT", "ADJUSTMENT", "SALE")
    & F.col("quantity_change").isNotNull() & F.col("event_timestamp").isNotNull()
    & F.col("ingested_at").isNotNull() & F.col("run_id").isNotNull()
)
validated_df = typed_df.withColumn("_is_valid", is_valid).cache()
total_count = validated_df.count()
valid_count = validated_df.filter("_is_valid").count()
logger.info(f"Inventory validation: total={total_count} valid={valid_count} invalid={total_count - valid_count}")

# Stable event-time / ingestion-time / run ordering makes duplicate copies of
# one event in a Bronze partition collapse to one append candidate.
batch_events = (
    validated_df.filter("_is_valid")
    .withColumn("_row_number", F.row_number().over(
        Window.partitionBy("inventory_event_id").orderBy(
            F.col("event_timestamp").desc(), F.col("ingested_at").desc(), F.col("run_id").desc()
        )
    ))
    .filter(F.col("_row_number") == 1)
    .drop("_row_number", "_is_valid")
    .withColumn("processed_timestamp", F.current_timestamp())
    .cache()
)
logger.info(f"Inventory events after batch deduplication: {batch_events.count()}")

spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog_name}.{silver_database}")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {silver_full_name} (
        inventory_event_id STRING, product_id STRING, warehouse_id STRING,
        movement_type STRING, quantity_change INT, event_timestamp TIMESTAMP,
        ingested_at TIMESTAMP, run_id STRING, processed_timestamp TIMESTAMP
    ) USING iceberg
""")
batch_events.createOrReplaceTempView("inventory_events_batch")

# This remains an append-event table. The anti join, rather than MERGE, makes a
# sequential replay of this Bronze partition unable to append the same event ID.
spark.sql(f"""
    INSERT INTO {silver_full_name}
    SELECT source.inventory_event_id, source.product_id, source.warehouse_id,
           source.movement_type, source.quantity_change, source.event_timestamp,
           source.ingested_at, source.run_id, source.processed_timestamp
    FROM inventory_events_batch AS source
    LEFT ANTI JOIN {silver_full_name} AS target
      ON source.inventory_event_id = target.inventory_event_id
""")

batch_events.unpersist()
validated_df.unpersist()
job.commit()
logger.info(f"Job {args['JOB_NAME']} finished at {datetime.utcnow().isoformat()}")
