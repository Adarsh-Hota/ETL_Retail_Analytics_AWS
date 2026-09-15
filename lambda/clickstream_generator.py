import random
import uuid
from datetime import datetime, timedelta


DEVICE_TYPES = ["mobile", "desktop", "tablet"]


def _event_record(
    session_id,
    customer_id,
    event_type,
    product_id,
    event_timestamp,
    page_url,
    device_type,
    run_id,
    ingested_at,
):
    return {
        "event_id": str(uuid.uuid4()),
        "event_timestamp": event_timestamp.isoformat(),
        "customer_id": customer_id,
        "session_id": session_id,
        "product_id": product_id,
        "event_type": event_type,
        "page_url": page_url,
        "device_type": device_type,
        "ingested_at": ingested_at or datetime.utcnow().isoformat(),
        "run_id": run_id or str(uuid.uuid4()),
    }


def generate_clickstream_events(
    customer_ids, product_ids, num_sessions=20, run_id=None, ingested_at=None
):
    """Generate browsing sessions, including anonymous and non-product events."""
    events = []

    for _ in range(num_sessions):
        session_id = f"SESS_{uuid.uuid4().hex[:12]}"
        customer_id = None if random.random() < 0.20 else random.choice(customer_ids)
        device_type = random.choice(DEVICE_TYPES)
        session_start = datetime.utcnow() - timedelta(minutes=random.randint(0, 60))
        product_id = random.choice(product_ids)

        sequence = [
            ("page_view", None, "/"),
            ("view_product", product_id, f"/products/{product_id}"),
        ]
        if random.random() < 0.70:
            sequence.append(("add_to_cart", product_id, "/cart"))
        if random.random() < 0.50:
            sequence.append(("checkout", product_id, "/checkout"))
        if random.random() < 0.35:
            sequence.append(("purchase", product_id, "/order-confirmation"))
        elif len(sequence) > 2 and random.random() < 0.20:
            sequence.append(("remove_from_cart", product_id, "/cart"))

        for index, (event_type, event_product_id, page_url) in enumerate(sequence):
            events.append(
                _event_record(
                    session_id,
                    customer_id,
                    event_type,
                    event_product_id,
                    session_start + timedelta(seconds=index * random.randint(10, 60)),
                    page_url,
                    device_type,
                    run_id,
                    ingested_at,
                )
            )

    return events


def generate_clickstream_from_orders(order_df, run_id=None, ingested_at=None):
    """Generate a plausible conversion path for each generated order."""
    events = []
    for _, order in order_df.iterrows():
        session_id = f"SESS_{uuid.uuid4().hex[:12]}"
        product_id = order["product_id"]
        base_time = datetime.fromisoformat(order["order_timestamp"])
        device_type = random.choice(DEVICE_TYPES)
        sequence = [
            ("view_product", f"/products/{product_id}"),
            ("add_to_cart", "/cart"),
            ("checkout", "/checkout"),
            ("purchase", "/order-confirmation"),
        ]
        for index, (event_type, page_url) in enumerate(sequence):
            events.append(
                _event_record(
                    session_id,
                    order["customer_id"],
                    event_type,
                    product_id,
                    base_time - timedelta(minutes=len(sequence) - index),
                    page_url,
                    device_type,
                    run_id,
                    ingested_at,
                )
            )

    return events
