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

# All fields are read as strings so malformed data can be validated and counted.
bronze_schema = StructType([
    StructField("source_event_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("operation", StringType(), True),
    StructField("source_updated_at", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("unit_price", StringType(), True),
    StructField("total_amount", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_timestamp", StringType(), True),
    StructField("ingested_at", StringType(), True),
    StructField("run_id", StringType(), True),
])

logger.info(f"Reading only supplied Bronze path: {bronze_path}")
bronze_df = spark.read.option("header", "true").schema(bronze_schema).csv(bronze_path)
typed_df = (
    bronze_df.select(*[F.trim(F.col(name)).alias(name) for name in bronze_schema.fieldNames()])
    .withColumn("operation", F.upper(F.col("operation")))
    .withColumn("source_updated_at", F.to_timestamp("source_updated_at"))
    .withColumn("order_timestamp", F.to_timestamp("order_timestamp"))
    .withColumn("ingested_at", F.to_timestamp("ingested_at"))
    .withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn("unit_price", F.col("unit_price").cast("double"))
    .withColumn("total_amount", F.col("total_amount").cast("double"))
)

common_valid = (
    F.col("source_event_id").isNotNull() & F.col("order_id").isNotNull()
    & F.col("operation").isin("I", "U", "D") & F.col("source_updated_at").isNotNull()
    & F.col("ingested_at").isNotNull() & F.col("run_id").isNotNull()
)
state_valid = (
    F.col("customer_id").isNotNull() & F.col("product_id").isNotNull()
    & F.col("quantity").isNotNull() & (F.col("quantity") > 0)
    & F.col("unit_price").isNotNull() & (F.col("unit_price") >= 0)
    & F.col("total_amount").isNotNull() & (F.col("total_amount") >= 0)
    & F.col("payment_method").isNotNull() & F.col("order_status").isNotNull()
    & F.col("order_timestamp").isNotNull()
)
# Deletes need the CDC envelope and business key only; omitted state attributes
# are intentionally accepted.
validated_df = typed_df.withColumn(
    "_is_valid", common_valid & ((F.col("operation") == "D") | state_valid)
).cache()
total_count = validated_df.count()
valid_count = validated_df.filter("_is_valid").count()
logger.info(f"Orders validation: total={total_count} valid={valid_count} invalid={total_count - valid_count}")

# source_event_id provides deterministic ordering for equal source timestamps.
latest_per_order = (
    validated_df.filter("_is_valid")
    .withColumn("_row_number", F.row_number().over(
        Window.partitionBy("order_id").orderBy(
            F.col("source_updated_at").desc(), F.col("source_event_id").desc()
        )
    ))
    .filter(F.col("_row_number") == 1)
    .drop("_row_number", "_is_valid")
    .withColumn("processed_timestamp", F.current_timestamp())
    .cache()
)
logger.info(f"Orders latest CDC events after deduplication: {latest_per_order.count()}")

spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog_name}.{silver_database}")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {silver_full_name} (
        order_id STRING, customer_id STRING, product_id STRING, quantity INT,
        unit_price DOUBLE, total_amount DOUBLE, payment_method STRING,
        order_status STRING, order_timestamp TIMESTAMP, source_event_id STRING,
        source_updated_at TIMESTAMP, ingested_at TIMESTAMP, run_id STRING,
        processed_timestamp TIMESTAMP
    ) USING iceberg
""")
latest_per_order.createOrReplaceTempView("orders_cdc_batch")

# The ordering predicates make both an exact replay and an older partition run
# unable to regress the latest state. Replayed deletes become harmless no-ops.
spark.sql(f"""
    MERGE INTO {silver_full_name} AS target
    USING orders_cdc_batch AS source
    ON target.order_id = source.order_id
    WHEN MATCHED AND source.operation = 'D'
      AND (source.source_updated_at > target.source_updated_at
           OR (source.source_updated_at = target.source_updated_at
               AND source.source_event_id >= target.source_event_id)) THEN DELETE
    WHEN MATCHED AND source.operation IN ('I', 'U')
      AND (source.source_updated_at > target.source_updated_at
           OR (source.source_updated_at = target.source_updated_at
               AND source.source_event_id >= target.source_event_id)) THEN UPDATE SET
        customer_id = source.customer_id, product_id = source.product_id,
        quantity = source.quantity, unit_price = source.unit_price,
        total_amount = source.total_amount, payment_method = source.payment_method,
        order_status = source.order_status, order_timestamp = source.order_timestamp,
        source_event_id = source.source_event_id, source_updated_at = source.source_updated_at,
        ingested_at = source.ingested_at, run_id = source.run_id,
        processed_timestamp = source.processed_timestamp
    WHEN NOT MATCHED AND source.operation IN ('I', 'U') THEN INSERT (
        order_id, customer_id, product_id, quantity, unit_price, total_amount,
        payment_method, order_status, order_timestamp, source_event_id,
        source_updated_at, ingested_at, run_id, processed_timestamp
    ) VALUES (
        source.order_id, source.customer_id, source.product_id, source.quantity,
        source.unit_price, source.total_amount, source.payment_method,
        source.order_status, source.order_timestamp, source.source_event_id,
        source.source_updated_at, source.ingested_at, source.run_id,
        source.processed_timestamp
    )
""")

latest_per_order.unpersist()
validated_df.unpersist()
job.commit()
logger.info(f"Job {args['JOB_NAME']} finished at {datetime.utcnow().isoformat()}")
