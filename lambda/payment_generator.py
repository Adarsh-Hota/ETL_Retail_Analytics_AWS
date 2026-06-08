import random
import uuid

import pandas as pd

from datetime import datetime, timedelta


PAYMENT_METHODS = [
    "UPI",
    "Credit Card",
    "Debit Card",
    "Net Banking",
    "Wallet"
]

PAYMENT_STATUSES = [
    "AUTHORIZED",
    "CAPTURED",
    "FAILED",
    "REFUNDED"
]


def generate_payments(num_records=100):

    payments = []

    for _ in range(num_records):

        amount = round(
            random.uniform(100, 10000),
            2
        )

        payment = {
            "payment_id": (
                f"PAY_{uuid.uuid4().hex[:8].upper()}"
            ),
            "order_id": (
                f"ORD_{uuid.uuid4().hex[:8].upper()}"
            ),
            "payment_method": random.choices(
                PAYMENT_METHODS,
                weights=[50, 20, 15, 10, 5]
            )[0],
            "payment_status": random.choices(
                PAYMENT_STATUSES,
                weights=[10, 75, 10, 5]
            )[0],
            "amount": amount,
            "currency": "INR",
            "payment_timestamp": (
                datetime.now()
                -
                timedelta(
                    minutes=random.randint(
                        1,
                        10080
                    )
                )
            ).isoformat()
        }

        payments.append(payment)

    return pd.DataFrame(payments)
