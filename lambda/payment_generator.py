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


def generate_payments(
    order_lookup,
    num_records=100,
    run_id=None,
    ingested_at=None,
    reference_timestamp=None,
    order_timestamp_lookup=None,
):

    payments = []

    for _ in range(num_records):

        order_id = random.choice(
            list(order_lookup.keys())
        )

        amount = order_lookup[order_id]

        source_updated_at = (
            reference_timestamp.replace(tzinfo=None).isoformat()
            if reference_timestamp is not None
            else datetime.utcnow().isoformat()
        )
        payment_timestamp = (
            order_timestamp_lookup[order_id]
            if reference_timestamp is not None and order_timestamp_lookup is not None
            else reference_timestamp.replace(tzinfo=None).isoformat()
            if reference_timestamp is not None
            else (
                datetime.now()
                - timedelta(minutes=random.randint(1, 10080))
            ).isoformat()
        )
        payment = {
            "source_event_id": str(uuid.uuid4()),

            "payment_id": (
                f"PAY_{uuid.uuid4().hex[:8].upper()}"
            ),

            "operation": "I",

            "source_updated_at": source_updated_at,
            "order_id": order_id,
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
            "payment_timestamp": payment_timestamp,
            "ingested_at": ingested_at or source_updated_at,
            "run_id": run_id or str(uuid.uuid4()),
        }

        payments.append(payment)

    return pd.DataFrame(payments)
