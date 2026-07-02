import random
import uuid
import pandas as pd

from datetime import datetime


def generate_orders(
        customer_ids,
        customer_lookup,
        product_lookup,
        num_records=100
    ):

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

        customer_id = random.choice(customer_ids)

        preferred_category = customer_lookup[customer_id]

        preferred_products = []

        other_products = []

        for product_id, product_info in product_lookup.items():

            if (product_info["category"]==preferred_category):
                preferred_products.append(product_id)

            else:
                other_products.append(product_id)

        if (preferred_products and random.random() < 0.7):
            candidate_products = preferred_products

        elif other_products:
            candidate_products = other_products

        else:
            candidate_products = preferred_products

        weights = [
            product_lookup[p]["popularity_score"]
            for p in candidate_products
        ]

        product_id = random.choices(
            candidate_products,
            weights=weights,
            k=1
        )[0]

        quantity = random.randint(1, 5)

        unit_price = product_lookup[
            product_id
        ]["price"]

        order = {
            "order_id": str(uuid.uuid4()),
            "customer_id": customer_id,
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
