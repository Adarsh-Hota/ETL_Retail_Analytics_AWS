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

# Read the known Bronze CSV contract directly so invalid records can be
# counted before type conversion and never require a crawler-created table.
bronze_schema = StructType([
    StructField("source_event_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("operation", StringType(), True),
    StructField("source_updated_at", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("signup_date", StringType(), True),
    StructField("loyalty_tier", StringType(), True),
    StructField("preferred_category", StringType(), True),
    StructField("ingested_at", StringType(), True),
    StructField("run_id", StringType(), True),
])

logger.info(f"Reading only supplied Bronze path: {bronze_path}")
bronze_df = spark.read.option("header", "true").schema(bronze_schema).csv(bronze_path)
typed_df = (
    bronze_df.select(*[F.trim(F.col(name)).alias(name) for name in bronze_schema.fieldNames()])
    .withColumn("operation", F.upper(F.col("operation")))
    .withColumn("email", F.lower(F.col("email")))
    .withColumn("source_updated_at", F.to_timestamp("source_updated_at"))
    .withColumn("ingested_at", F.to_timestamp("ingested_at"))
    .withColumn("signup_date", F.to_date("signup_date"))
)

common_valid = (
    F.col("source_event_id").isNotNull() & F.col("customer_id").isNotNull()
    & F.col("operation").isin("I", "U", "D") & F.col("source_updated_at").isNotNull()
    & F.col("ingested_at").isNotNull() & F.col("run_id").isNotNull()
)
state_valid = (
    F.col("first_name").isNotNull() & F.col("last_name").isNotNull()
    & F.col("email").isNotNull() & F.col("email").rlike(r"^[^@ ]+@[^@ ]+[.][^@ ]+$")
    & F.col("city").isNotNull() & F.col("state").isNotNull()
    & F.col("signup_date").isNotNull()
    & F.col("loyalty_tier").isin("Bronze", "Silver", "Gold", "Platinum")
    & F.col("preferred_category").isin("Electronics", "Fashion", "Home", "Sports", "Books")
)
# A valid delete only needs the mutable-domain envelope and business key.
validated_df = typed_df.withColumn(
    "_is_valid", common_valid & ((F.col("operation") == "D") | state_valid)
).cache()
total_count = validated_df.count()
valid_count = validated_df.filter("_is_valid").count()
logger.info(f"Customers validation: total={total_count} valid={valid_count} invalid={total_count - valid_count}")

# source_event_id deterministically breaks source timestamp ties.
latest_per_customer = (
    validated_df.filter("_is_valid")
    .withColumn("_row_number", F.row_number().over(
        Window.partitionBy("customer_id").orderBy(
            F.col("source_updated_at").desc(), F.col("source_event_id").desc()
        )
    ))
    .filter(F.col("_row_number") == 1)
    .drop("_row_number", "_is_valid")
    .withColumn("processed_timestamp", F.current_timestamp())
    .cache()
)
logger.info(f"Customers latest CDC events after deduplication: {latest_per_customer.count()}")

spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog_name}.{silver_database}")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {silver_full_name} (
        customer_id STRING, first_name STRING, last_name STRING, email STRING,
        city STRING, state STRING, signup_date DATE, loyalty_tier STRING,
        preferred_category STRING, source_event_id STRING,
        source_updated_at TIMESTAMP, ingested_at TIMESTAMP, run_id STRING,
        effective_from TIMESTAMP, effective_to TIMESTAMP, is_current BOOLEAN,
        processed_timestamp TIMESTAMP
    ) USING iceberg
""")
latest_per_customer.createOrReplaceTempView("customers_cdc_batch")

# Only a newer source event expires a current version. D expires it but creates
# no new row; I and U establish a new full-state version when appropriate.
spark.sql(f"""
    MERGE INTO {silver_full_name} AS target
    USING customers_cdc_batch AS source
    ON target.customer_id = source.customer_id AND target.is_current = true
    WHEN MATCHED
      AND (source.source_updated_at > target.source_updated_at
           OR (source.source_updated_at = target.source_updated_at
               AND source.source_event_id > target.source_event_id)) THEN UPDATE SET
        effective_to = source.source_updated_at,
        is_current = false
""")

# A current equal/newer source version prevents late regressions. Existing
# source_event_id history blocks exact Bronze-partition replays from adding a
# duplicate historical version after an update has already expired its parent.
spark.sql(f"""
    INSERT INTO {silver_full_name}
    SELECT source.customer_id, source.first_name, source.last_name, source.email,
           source.city, source.state, source.signup_date, source.loyalty_tier,
           source.preferred_category, source.source_event_id,
           source.source_updated_at, source.ingested_at, source.run_id,
           source.source_updated_at, CAST(NULL AS TIMESTAMP), true,
           source.processed_timestamp
    FROM customers_cdc_batch AS source
    LEFT ANTI JOIN {silver_full_name} AS current_target
      ON source.customer_id = current_target.customer_id
     AND current_target.is_current = true
     AND (current_target.source_updated_at > source.source_updated_at
          OR (current_target.source_updated_at = source.source_updated_at
              AND current_target.source_event_id >= source.source_event_id))
    LEFT ANTI JOIN {silver_full_name} AS existing_event
      ON source.customer_id = existing_event.customer_id
     AND source.source_event_id = existing_event.source_event_id
    WHERE source.operation IN ('I', 'U')
""")

latest_per_customer.unpersist()
validated_df.unpersist()
job.commit()
logger.info(f"Job {args['JOB_NAME']} finished at {datetime.utcnow().isoformat()}")
