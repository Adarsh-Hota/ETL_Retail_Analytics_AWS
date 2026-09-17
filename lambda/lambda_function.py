import boto3
import hashlib
import json
import os
import pandas as pd
import time
import uuid

from datetime import datetime
from faker import Faker
from cleanup_utils import (
    reset_data_lake
)
from clickstream_generator import (
    generate_clickstream_events,
    generate_clickstream_from_orders
)
from customer_generator import generate_customers
from inventory_generator import (
    generate_inventory_events,
    generate_inventory_from_orders
)
from orders_generator import generate_orders
from payment_generator import generate_payments
from product_generator import generate_products
from s3_utils import (
    upload_dataframe_to_s3,
    upload_json_lines_to_s3,
)


fake = Faker()

s3 = boto3.client("s3")

BUCKET_NAME = os.environ["BUCKET_NAME"]
    
RESET_DATA_LAKE = (
    os.environ.get("RESET_DATA_LAKE", "false").lower() == "true"
)

KINESIS_MAX_RECORDS = 500
KINESIS_MAX_RECORD_BYTES = 1024 * 1024
KINESIS_MAX_BATCH_BYTES = 5 * 1024 * 1024
CLICKSTREAM_INGESTION_MODES = {"kinesis", "direct_s3"}


def clickstream_ingestion_mode():
    """Read the explicit Clickstream delivery mode without changing the default."""
    mode = os.environ.get("CLICKSTREAM_INGESTION_MODE", "kinesis").strip().lower()
    if mode not in CLICKSTREAM_INGESTION_MODES:
        raise ValueError(
            "CLICKSTREAM_INGESTION_MODE must be one of: kinesis, direct_s3"
        )
    return mode


def clickstream_partition_key(event):
    """Return a stable Kinesis key, preferring session affinity."""
    candidate = event.get("session_id") or event.get("customer_id")
    if not candidate:
        event_id = str(event.get("event_id", ""))
        candidate = f"anonymous-{hashlib.sha256(event_id.encode('utf-8')).hexdigest()}"
    candidate = str(candidate)
    if len(candidate.encode("utf-8")) > 256:
        return hashlib.sha256(candidate.encode("utf-8")).hexdigest()
    return candidate


def build_kinesis_record_batches(events):
    """Serialize events and group them within Kinesis PutRecords limits."""
    batches = []
    batch = []
    batch_bytes = 0
    for event in events:
        # The newline lets a Firehose S3 destination retain NDJSON framing.
        payload = (json.dumps(event, separators=(",", ":"), sort_keys=True) + "\n").encode("utf-8")
        partition_key = clickstream_partition_key(event)
        record_size = len(payload) + len(partition_key.encode("utf-8"))
        if record_size > KINESIS_MAX_RECORD_BYTES:
            raise ValueError(f"Clickstream event {event.get('event_id')} exceeds the Kinesis record size limit")
        if batch and (len(batch) == KINESIS_MAX_RECORDS or batch_bytes + record_size > KINESIS_MAX_BATCH_BYTES):
            batches.append(batch)
            batch = []
            batch_bytes = 0
        batch.append({"Data": payload, "PartitionKey": partition_key})
        batch_bytes += record_size
    if batch:
        batches.append(batch)
    return batches


def put_clickstream_records(events, stream_name, kinesis_client, max_retries=3, sleep_fn=time.sleep):
    """Put all events, retrying only records Kinesis reports as failed."""
    if not stream_name:
        raise ValueError("CLICKSTREAM_STREAM_NAME must be configured")
    if max_retries < 1:
        raise ValueError("max_retries must be at least 1")

    delivered = 0
    for batch_number, batch in enumerate(build_kinesis_record_batches(events), start=1):
        pending = batch
        for attempt in range(1, max_retries + 1):
            responses = []
            try:
                response = kinesis_client.put_records(StreamName=stream_name, Records=pending)
                responses = response.get("Records", [])
                if len(responses) != len(pending):
                    raise RuntimeError("Kinesis returned a record response count that does not match the request")
                failed = [
                    record for record, result in zip(pending, responses)
                    if result.get("ErrorCode")
                ]
                delivered += len(pending) - len(failed)
            except Exception as error:
                print(f"Kinesis batch {batch_number} attempt {attempt} raised {type(error).__name__}: {error}")
                failed = pending

            if not failed:
                break

            error_codes = sorted({result.get("ErrorCode") for result in responses if result.get("ErrorCode")}) or ["exception"]
            print(f"Kinesis batch {batch_number} attempt {attempt} failed_records={len(failed)} error_codes={error_codes}")
            pending = failed
            if attempt < max_retries:
                sleep_fn(0.1 * attempt)
        else:
            raise RuntimeError(
                f"Could not deliver {len(pending)} clickstream records from Kinesis batch {batch_number} "
                f"after {max_retries} attempts"
            )
    return delivered



def lambda_handler(event, context):

    # Validate before generating or uploading a batch. direct_s3 is deliberately
    # limited to a low-cost test path; Kinesis remains the default target path.
    selected_clickstream_mode = clickstream_ingestion_mode()

    if RESET_DATA_LAKE:

        print(
            "Resetting bronze, silver and gold layers..."
        )

        reset_data_lake(
            BUCKET_NAME
        )

        return {
            "statusCode": 200,
            "message": (
                "Bronze, Silver and Gold "
                "layers successfully reset."
            )
        }
    
    now = datetime.now()
    run_id = str(uuid.uuid4())
    ingested_at = datetime.utcnow().isoformat()

    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")

    customer_df = generate_customers(
        100,
        run_id=run_id,
        ingested_at=ingested_at,
    )

    customer_ids = (
        customer_df["customer_id"]
        .tolist()
    )

    customer_lookup = (
        customer_df
        .set_index("customer_id")
        ["preferred_category"]
        .to_dict()
    )

    product_df = generate_products(
        100,
        run_id=run_id,
        ingested_at=ingested_at,
    )

    product_ids = (
        product_df["product_id"]
        .tolist()
    )

    product_lookup = (
        product_df
        .set_index("product_id")
        [["price", "popularity_score", "category"]]
        .to_dict("index")
    )

    order_df = generate_orders(
        customer_ids=customer_ids,
        customer_lookup=customer_lookup,
        product_lookup=product_lookup,
        num_records=100,
        run_id=run_id,
        ingested_at=ingested_at,
    )

    order_lookup = (
        order_df
        .set_index("order_id")
        ["total_amount"]
        .to_dict()
    )

    payment_df = generate_payments(
        order_lookup,
        num_records=100,
        run_id=run_id,
        ingested_at=ingested_at,
    )

    operational_inventory_df = (
        generate_inventory_events(
            product_ids,
            100,
            run_id=run_id,
            ingested_at=ingested_at,
        )
    )

    sales_inventory_df = (
        generate_inventory_from_orders(
            order_df,
            run_id=run_id,
            ingested_at=ingested_at,
        )
    )

    inventory_df = pd.concat(
        [
            operational_inventory_df,
            sales_inventory_df
        ],
        ignore_index=True
    )

    random_clickstream_events = generate_clickstream_events(
            customer_ids=customer_ids,
            product_ids=product_ids,
            num_sessions=20,
            run_id=run_id,
            ingested_at=ingested_at,
    )

    order_clickstream_events = (
        generate_clickstream_from_orders(
            order_df,
            run_id=run_id,
            ingested_at=ingested_at,
        )
    )

    clickstream_events = (
        random_clickstream_events
        +
        order_clickstream_events
    )

    customer_s3_path = (
        upload_dataframe_to_s3(
            customer_df,
            "customers",
            year,
            month,
            day,
            s3,
            BUCKET_NAME
        )
    )

    product_s3_path = (
        upload_dataframe_to_s3(
            product_df,
            "products",
            year,
            month,
            day,
            s3,
            BUCKET_NAME
        )
    )

    order_s3_path = (
        upload_dataframe_to_s3(
            order_df,
            "orders",
            year,
            month,
            day,
            s3,
            BUCKET_NAME
        )
    )

    payment_s3_path = (
        upload_dataframe_to_s3(
            payment_df,
            "payments",
            year,
            month,
            day,
            s3,
            BUCKET_NAME
        )
    )

    inventory_s3_path = (
        upload_dataframe_to_s3(
            inventory_df,
            "inventory",
            year,
            month,
            day,
            s3,
            BUCKET_NAME
        )
    )

    print(
        "Clickstream selected_mode=%s run_id=%s generated_records=%s"
        % (selected_clickstream_mode, run_id, len(clickstream_events))
    )
    clickstream_result = {}
    if selected_clickstream_mode == "direct_s3":
        clickstream_s3_path = upload_json_lines_to_s3(
            clickstream_events,
            "clickstream",
            year,
            month,
            day,
            s3,
            BUCKET_NAME,
        )
        clickstream_result = {
            "clickstream_bronze_path": f"s3://{BUCKET_NAME}/{clickstream_s3_path}",
            "clickstream_records_written": len(clickstream_events),
        }
        print(
            "Clickstream direct_s3 run_id=%s written_records=%s bronze_path=%s"
            % (run_id, len(clickstream_events), clickstream_result["clickstream_bronze_path"])
        )
    else:
        # Keep Kinesis batching, retries, partitioning, and error behavior intact.
        clickstream_stream_name = os.environ["CLICKSTREAM_STREAM_NAME"]
        clickstream_records_delivered = put_clickstream_records(
            clickstream_events,
            clickstream_stream_name,
            boto3.client("kinesis"),
        )
        clickstream_result = {
            "clickstream_stream": clickstream_stream_name,
            "clickstream_records_delivered": clickstream_records_delivered,
        }
        print(
            "Clickstream kinesis run_id=%s delivered_records=%s stream=%s"
            % (run_id, clickstream_records_delivered, clickstream_stream_name)
        )

    result = {
        "statusCode": 200,

        "run_id": run_id,

        "clickstream_ingestion_mode": selected_clickstream_mode,

        "customers_uploaded": (
            f"s3://{BUCKET_NAME}/{customer_s3_path}"
        ),

        "products_uploaded": (
            f"s3://{BUCKET_NAME}/{product_s3_path}"
        ),

        "orders_uploaded": (
            f"s3://{BUCKET_NAME}/{order_s3_path}"
        ),

        "payments_uploaded": (
            f"s3://{BUCKET_NAME}/{payment_s3_path}"
        ),

        "inventory_uploaded": (
            f"s3://{BUCKET_NAME}/{inventory_s3_path}"
        ),

        "customers_generated": len(
            customer_df
        ),

        "products_generated": len(
            product_df
        ),

        "orders_generated": len(
            order_df
        ),

        "payments_generated": len(
            payment_df
        ),

        "inventory_generated": len(
            inventory_df
        ),

        "clickstream_events_generated": len(
            clickstream_events
        )
    }
    result.update(clickstream_result)
    return result
