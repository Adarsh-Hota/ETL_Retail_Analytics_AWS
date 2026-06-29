import random
import uuid
import pandas as pd

from datetime import datetime


def generate_orders(customer_ids, product_lookup, num_records=100):

    orders = []

    payment_methods = [
        "UPI",
        "Credit Card",
        "Debit Card",
        "Net Banking"
    ]

    order_statuses = [
        "Placed",
        "Shipped",
        "Delivered",
        "Cancelled"
    ]

    for _ in range(num_records):

        product_ids = list(product_lookup.keys())
        weights = [
            product_lookup[p]["popularity_score"]
            for p in product_ids
        ]
        product_id = random.choices(
            product_ids,
            weights=weights,
            k=1
        )[0]
        quantity = random.randint(1, 5)
        unit_price = product_lookup[
            product_id
        ]["price"]

        order = {
            "order_id": str(uuid.uuid4()),
            "customer_id": random.choice(customer_ids),
            "product_id": product_id,
            "quantity": quantity,
            "unit_price": unit_price,
            "total_amount": round(quantity * unit_price, 2),
            "payment_method": random.choice(payment_methods),
            "order_status": random.choice(order_statuses),
            "order_timestamp": datetime.now().isoformat()
        }

        orders.append(order)

    return pd.DataFrame(orders)
