import boto3
import json
import os
import uuid

from datetime import datetime
from faker import Faker

from clickstream_generator import generate_clickstream_events
from customer_generator import generate_customers
from inventory_generator import generate_inventory_events
from orders_generator import generate_orders
from payment_generator import generate_payments
from product_generator import generate_products


fake = Faker()

s3 = boto3.client("s3")

BUCKET_NAME = os.environ["BUCKET_NAME"]


def lambda_handler(event, context):
    
    now = datetime.now()

    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")

    customer_df = generate_customers(100)

    customer_ids = (
        customer_df["customer_id"]
        .tolist()
    )

    product_df = generate_products(100)

    product_ids = (
        product_df["product_id"]
        .tolist()
    )

    product_lookup = (
        product_df
        .set_index("product_id")
        [["price", "popularity_score"]]
        .to_dict("index")
    )

    order_df = generate_orders(
        customer_ids=customer_ids, 
        product_lookup=product_lookup, 
        num_records=100
    )

    order_ids = (
        order_df["order_id"]
        .tolist()
    )

    order_lookup = (
        order_df
        .set_index("order_id")
        ["total_amount"]
        .to_dict()
    )

    payment_df = generate_payments(
        order_lookup,
        num_records=100
    )

    inventory_df = generate_inventory_events(
        product_ids=product_ids, 
        num_records=100
    )

    clickstream_events = generate_clickstream_events(
            customer_ids=customer_ids,
            product_ids=product_ids,
            num_sessions=20
    )

    customer_filename = (
    f"customers_{uuid.uuid4().hex}.csv"
    )

    customer_local_path = (
        f"/tmp/{customer_filename}"
    )

    customer_s3_path = (
        f"bronze/customers/"
        f"year={year}/"
        f"month={month}/"
        f"day={day}/"
        f"{customer_filename}"
    )

    customer_df.to_csv(
        customer_local_path,
        index=False
    )

    s3.upload_file(
        customer_local_path,
        BUCKET_NAME,
        customer_s3_path
    )

    product_filename = (
        f"products_{uuid.uuid4().hex}.csv"
    )

    product_local_path = (
        f"/tmp/{product_filename}"
    )

    product_s3_path = (
        f"bronze/products/"
        f"year={year}/"
        f"month={month}/"
        f"day={day}/"
        f"{product_filename}"
    )

    product_df.to_csv(
        product_local_path,
        index=False
    )

    s3.upload_file(
        product_local_path,
        BUCKET_NAME,
        product_s3_path
    )

    order_filename = f"orders_{uuid.uuid4().hex}.csv"

    local_path = f"/tmp/{order_filename}"

    s3_path = (
        f"bronze/orders/"
        f"year={year}/"
        f"month={month}/"
        f"day={day}/"
        f"{order_filename}"
    )

    # Save CSV locally
    df.to_csv(local_path, index=False)

    # Upload to S3
    s3.upload_file(
        local_path,
        BUCKET_NAME,
        s3_path
    )

    payment_filename = (
        f"payments_{uuid.uuid4().hex}.csv"
    )

    payment_filename = (
        f"payments_{uuid.uuid4().hex}.csv"
    )

    payment_local_path = (
        f"/tmp/{payment_filename}"
    )

    payment_s3_path = (
        f"bronze/payments/"
        f"year={year}/"
        f"month={month}/"
        f"day={day}/"
        f"{payment_filename}"
    )

    payment_df.to_csv(
        payment_local_path,
        index=False
    )

    s3.upload_file(
        payment_local_path,
        BUCKET_NAME,
        payment_s3_path
    )

    inventory_filename = f"inventory_{uuid.uuid4().hex}.csv"

    inventory_local_path = f"/tmp/{inventory_filename}"

    inventory_s3_path = (
        f"bronze/inventory/year={year}/"
        f"month={month}/"
        f"day={day}/"
        f"{inventory_filename}"
    )

    inventory_df.to_csv(
        inventory_local_path,
        index=False
    )

    s3.upload_file(
        inventory_local_path,
        BUCKET_NAME,
        inventory_s3_path
    )

    clickstream_filename = (
        f"clickstream_{uuid.uuid4().hex}.json"
    )

    clickstream_local_path = (
        f"/tmp/{clickstream_filename}"
    )

    clickstream_s3_path = (
        f"bronze/clickstream/"
        f"year={year}/"
        f"month={month}/"
        f"day={day}/"
        f"{clickstream_filename}"
    )

    with open(clickstream_local_path, "w") as f:

        for event in clickstream_events:
            f.write(json.dumps(event) + "\n")

    s3.upload_file(
        clickstream_local_path,
        BUCKET_NAME,
        clickstream_s3_path
    )

    return {
        "statusCode": 200,
        "orders_uploaded": f"s3://{BUCKET_NAME}/{s3_path}",
        "clickstream_uploaded": f"s3://{BUCKET_NAME}/{clickstream_s3_path}",
        "customers_uploaded": f"s3://{BUCKET_NAME}/{customer_s3_path}",
        "inventroy_uploaded": f"s3://{BUCKET_NAME}/{inventory_s3_path}",
        "payments_uploaded": f"s3://{BUCKET_NAME}/{payment_s3_path}",
        "products_uploaded": f"s3://{BUCKET_NAME}/{product_s3_path}",
        "orders_generated": len(df),
        "clickstream_events_generated": len(clickstream_events),
        "customers_generated": len(customer_df),
        "inventroy_generated": len(inventory_df),
        "payments_generated": len(payment_df),
        "products_generated": len(product_df)
    }
