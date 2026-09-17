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

# Read known Bronze CSV fields directly. Strings preserve malformed input long
# enough to count it before casting and validation.
bronze_schema = StructType([
    StructField("source_event_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("operation", StringType(), True),
    StructField("source_updated_at", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("subcategory", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("cost_price", StringType(), True),
    StructField("price", StringType(), True),
    StructField("popularity_score", StringType(), True),
    StructField("launch_date", StringType(), True),
    StructField("is_active", StringType(), True),
    StructField("ingested_at", StringType(), True),
    StructField("run_id", StringType(), True),
])

logger.info(f"Reading only supplied Bronze path: {bronze_path}")
bronze_df = spark.read.option("header", "true").schema(bronze_schema).csv(bronze_path)
typed_df = (
    bronze_df.select(*[F.trim(F.col(name)).alias(name) for name in bronze_schema.fieldNames()])
    .withColumn("operation", F.upper(F.col("operation")))
    .withColumn("source_updated_at", F.to_timestamp("source_updated_at"))
    .withColumn("ingested_at", F.to_timestamp("ingested_at"))
    .withColumn("cost_price", F.col("cost_price").cast("double"))
    .withColumn("price", F.col("price").cast("double"))
    .withColumn("popularity_score", F.col("popularity_score").cast("double"))
    .withColumn("launch_date", F.to_date("launch_date"))
    .withColumn("is_active", F.col("is_active").cast("boolean"))
)

common_valid = (
    F.col("source_event_id").isNotNull() & F.col("product_id").isNotNull()
    & F.col("operation").isin("I", "U", "D") & F.col("source_updated_at").isNotNull()
    & F.col("ingested_at").isNotNull() & F.col("run_id").isNotNull()
)
state_valid = (
    F.col("product_name").isNotNull() & F.col("category").isNotNull()
    & F.col("subcategory").isNotNull() & F.col("brand").isNotNull()
    & F.col("cost_price").isNotNull() & (F.col("cost_price") >= 0)
    & F.col("price").isNotNull() & (F.col("price") >= 0)
    & F.col("popularity_score").isNotNull() & (F.col("popularity_score") >= 0)
    & F.col("launch_date").isNotNull() & F.col("is_active").isNotNull()
)
# A delete needs its CDC envelope and product key, but not a full product state.
validated_df = typed_df.withColumn(
    "_is_valid", common_valid & ((F.col("operation") == "D") | state_valid)
).cache()
total_count = validated_df.count()
valid_count = validated_df.filter("_is_valid").count()
logger.info(f"Products validation: total={total_count} valid={valid_count} invalid={total_count - valid_count}")

# source_event_id resolves equal source timestamps deterministically.
latest_per_product = (
    validated_df.filter("_is_valid")
    .withColumn("_row_number", F.row_number().over(
        Window.partitionBy("product_id").orderBy(
            F.col("source_updated_at").desc(), F.col("source_event_id").desc()
        )
    ))
    .filter(F.col("_row_number") == 1)
    .drop("_row_number", "_is_valid")
    .withColumn("processed_timestamp", F.current_timestamp())
    .cache()
)
logger.info(f"Products latest CDC events after deduplication: {latest_per_product.count()}")

spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog_name}.{silver_database}")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {silver_full_name} (
        product_id STRING, product_name STRING, category STRING, subcategory STRING,
        brand STRING, cost_price DOUBLE, price DOUBLE, popularity_score DOUBLE,
        launch_date DATE, is_active BOOLEAN, source_event_id STRING,
        source_updated_at TIMESTAMP, ingested_at TIMESTAMP, run_id STRING,
        effective_from TIMESTAMP, effective_to TIMESTAMP, is_current BOOLEAN,
        processed_timestamp TIMESTAMP
    ) USING iceberg
""")
latest_per_product.createOrReplaceTempView("products_cdc_batch")

# Expire a current row only when the incoming event is later. Applying this to
# I and U permits either source operation to establish a newer full state; D
# expires the current version and deliberately creates no replacement.
spark.sql(f"""
    MERGE INTO {silver_full_name} AS target
    USING products_cdc_batch AS source
    ON target.product_id = source.product_id AND target.is_current = true
    WHEN MATCHED
      AND (source.source_updated_at > target.source_updated_at
           OR (source.source_updated_at = target.source_updated_at
               AND source.source_event_id > target.source_event_id)) THEN UPDATE SET
        effective_to = source.source_updated_at,
        is_current = false
""")

# Insert a new SCD2 version only when no equal/newer current version remains
# and this exact source event was not already preserved in history. The first
# condition prevents late-event regressions; the second prevents replayed
# partitions from duplicating historical versions after an earlier expiration.
spark.sql(f"""
    INSERT INTO {silver_full_name}
    SELECT source.product_id, source.product_name, source.category,
           source.subcategory, source.brand, source.cost_price, source.price,
           source.popularity_score, source.launch_date, source.is_active,
           source.source_event_id, source.source_updated_at, source.ingested_at,
           source.run_id, source.source_updated_at, CAST(NULL AS TIMESTAMP),
           true, source.processed_timestamp
    FROM products_cdc_batch AS source
    LEFT ANTI JOIN {silver_full_name} AS current_target
      ON source.product_id = current_target.product_id
     AND current_target.is_current = true
     AND (current_target.source_updated_at > source.source_updated_at
          OR (current_target.source_updated_at = source.source_updated_at
              AND current_target.source_event_id >= source.source_event_id))
    LEFT ANTI JOIN {silver_full_name} AS existing_event
      ON source.product_id = existing_event.product_id
     AND source.source_event_id = existing_event.source_event_id
    WHERE source.operation IN ('I', 'U')
""")

latest_per_product.unpersist()
validated_df.unpersist()
job.commit()
logger.info(f"Job {args['JOB_NAME']} finished at {datetime.utcnow().isoformat()}")
