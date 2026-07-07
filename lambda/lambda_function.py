import boto3
import json
import os
import pandas as pd
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
    upload_json_lines_to_s3
)


fake = Faker()

s3 = boto3.client("s3")

BUCKET_NAME = os.environ["BUCKET_NAME"]
    
RESET_DATA_LAKE = (
    os.environ.get("RESET_DATA_LAKE", "false").lower() == "true"
)



def lambda_handler(event, context):

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

    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")

    customer_df = generate_customers(100)

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

    product_df = generate_products(100)

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
        num_records=100
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

    operational_inventory_df = (
        generate_inventory_events(
            product_ids,
            100
        )
    )

    sales_inventory_df = (
        generate_inventory_from_orders(
            order_df
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
            num_sessions=20
    )

    order_clickstream_events = (
        generate_clickstream_from_orders(
            order_df
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

    clickstream_s3_path = (
        upload_json_lines_to_s3(
            clickstream_events,
            "clickstream",
            year,
            month,
            day,
            s3,
            BUCKET_NAME
        )
    )

    return {
        "statusCode": 200,

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

        "clickstream_uploaded": (
            f"s3://{BUCKET_NAME}/{clickstream_s3_path}"
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
