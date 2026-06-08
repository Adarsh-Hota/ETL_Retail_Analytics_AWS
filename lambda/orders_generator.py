import random
import uuid
import pandas as pd

from datetime import datetime


def generate_orders(num_records=100):

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

        quantity = random.randint(1, 5)
        unit_price = round(random.uniform(100, 5000), 2)

        order = {
            "order_id": str(uuid.uuid4()),
            "customer_id": f"CUST_{random.randint(1000, 9999)}",
            "product_id": f"PROD_{random.randint(100, 999)}",
            "quantity": quantity,
            "unit_price": unit_price,
            "total_amount": round(quantity * unit_price, 2),
            "payment_method": random.choice(payment_methods),
            "order_status": random.choice(order_statuses),
            "order_timestamp": datetime.now().isoformat()
        }

        orders.append(order)

    return pd.DataFrame(orders)
