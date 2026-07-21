import sys
from datetime import datetime

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType
)

# ---------------------------------------------------------------------------
# 0. Job / Glue / Iceberg setup
# ---------------------------------------------------------------------------

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "bronze_path", "catalog_name", "silver_database", "silver_table"],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = glueContext.get_logger()
logger.info(f"Starting job {args['JOB_NAME']}")

CATALOG = args["catalog_name"]          # e.g. glue_catalog
SILVER_DB = args["silver_database"]     # e.g. silver
SILVER_TABLE = args["silver_table"]     # e.g. products
BRONZE_PATH = args["bronze_path"]       # e.g. s3://my-bucket/bronze/products/

FULL_TABLE_NAME = f"{CATALOG}.{SILVER_DB}.{SILVER_TABLE}"


# ---------------------------------------------------------------------------
# Create the Silver Iceberg table if it doesn't exist yet
# ---------------------------------------------------------------------------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {SILVER_DB}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {FULL_TABLE_NAME} (
        product_id          STRING,
        product_name        STRING,
        category            STRING,
        subcategory         STRING,
        brand               STRING,
        price               DOUBLE,
        popularity_score    DOUBLE,
        is_active           BOOLEAN,
        effective_from      TIMESTAMP,
        effective_to        TIMESTAMP,
        is_current          BOOLEAN,
        processed_timestamp TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (is_current)
""")

# ---------------------------------------------------------------------------
# Read Bronze data
# ---------------------------------------------------------------------------

bronze_schema = StructType([
    StructField("product_id", StringType()),
    StructField("product_name", StringType()),
    StructField("category", StringType()),
    StructField("subcategory", StringType()),
    StructField("brand", StringType()),
    StructField("price", StringType()),
    StructField("popularity_score", StringType()),
    StructField("is_active", StringType()),
    StructField("operation", StringType()),
    StructField("source_updated_at", StringType()),
])

bronze_df = (
    spark.read
    .option("header", "true")
    .schema(bronze_schema)
    .csv(BRONZE_PATH)
)

# ---------------------------------------------------------------------------
# Clean and cast columns
# ---------------------------------------------------------------------------

bronze_clean_df = (
    bronze_df
    .withColumn("product_id", F.trim(F.col("product_id")))
    .withColumn("product_name", F.trim(F.col("product_name")))
    .withColumn("category", F.trim(F.col("category")))
    .withColumn("subcategory", F.trim(F.col("subcategory")))
    .withColumn("brand", F.trim(F.col("brand")))
    .withColumn("price", F.col("price").cast(DoubleType()))
    .withColumn("popularity_score", F.col("popularity_score").cast(DoubleType()))
    .withColumn("is_active", F.col("is_active").cast(BooleanType()))
    .withColumn("operation", F.upper(F.trim(F.col("operation"))))
    .withColumn("source_updated_at", F.to_timestamp(F.col("source_updated_at")))
    .withColumn("processed_timestamp", F.current_timestamp())
    # Drop any rows with no product_id or no operation -- can't process them
    .filter(F.col("product_id").isNotNull() & F.col("operation").isNotNull())
)

# If the same product_id appears more than once in this batch, keep only the
# latest event per product_id based on source_updated_at.
dedup_window = Window.partitionBy("product_id").orderBy(F.col("source_updated_at").desc())

latest_per_product_df = (
    bronze_clean_df
    .withColumn("_rn", F.row_number().over(dedup_window))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

latest_per_product_df.cache()

# ---------------------------------------------------------------------------
# Split into insert / update / delete DataFrames
# ---------------------------------------------------------------------------

insert_df = latest_per_product_df.filter(F.col("operation") == "I")
update_df = latest_per_product_df.filter(F.col("operation") == "U")
delete_df = latest_per_product_df.filter(F.col("operation") == "D")

# ---------------------------------------------------------------------------
# Apply MERGE statements (Iceberg row-level operations)
# ---------------------------------------------------------------------------
#
# For UPDATE and DELETE we only need to expire the currently-active silver
# row. We register the relevant slice of the batch as a temp view and run a
# MERGE against the Iceberg table, matching on product_id + is_current=true.
# This avoids rewriting the whole table -- Iceberg only rewrites the
# affected data files.

# Expire current rows for UPDATE events
update_df.createOrReplaceTempView("updates_batch")

spark.sql(f"""
    MERGE INTO {FULL_TABLE_NAME} AS target
    USING updates_batch AS source
    ON target.product_id = source.product_id AND target.is_current = true
    WHEN MATCHED THEN
      UPDATE SET
        target.effective_to = source.source_updated_at,
        target.is_current   = false
""")

# Expire current rows for DELETE events (no new row inserted afterwards)
delete_df.createOrReplaceTempView("deletes_batch")

spark.sql(f"""
    MERGE INTO {FULL_TABLE_NAME} AS target
    USING deletes_batch AS source
    ON target.product_id = source.product_id AND target.is_current = true
    WHEN MATCHED THEN
      UPDATE SET
        target.effective_to = source.source_updated_at,
        target.is_current   = false
""")

# ---------------------------------------------------------------------------
# Insert new current records (for both INSERT and UPDATE events)
# ---------------------------------------------------------------------------
#
# DELETE events do not get a new row -- the table simply ends up with no
# current version for that product_id.

new_rows_df = (
    insert_df.unionByName(update_df)
    .select(
        "product_id",
        "product_name",
        "category",
        "subcategory",
        "brand",
        "price",
        "popularity_score",
        "is_active",
        F.col("source_updated_at").alias("effective_from"),
        F.lit(None).cast("timestamp").alias("effective_to"),
        F.lit(True).alias("is_current"),
        "processed_timestamp",
    )
)

new_rows_df.createOrReplaceTempView("new_rows_batch")

spark.sql(f"""
    INSERT INTO {FULL_TABLE_NAME}
    SELECT
        product_id,
        product_name,
        category,
        subcategory,
        brand,
        price,
        popularity_score,
        is_active,
        effective_from,
        effective_to,
        is_current,
        processed_timestamp
    FROM new_rows_batch
""")

# ---------------------------------------------------------------------------
# Commit the Glue job
# ---------------------------------------------------------------------------

latest_per_product_df.unpersist()
job.commit()
logger.info(f"Job {args['JOB_NAME']} finished successfully at {datetime.utcnow().isoformat()}")
