import random
import uuid

from datetime import datetime, timedelta

EVENT_TYPES = [
    "view_product",
    "add_to_cart",
    "remove_from_cart",
    "checkout",
    "purchase"
]

def generate_clickstream_events(customer_ids, product_ids, num_sessions=20):

    events = []

    for _ in range(num_sessions):

        session_id = f"SESS_{uuid.uuid4().hex[:8]}"
        
        customer_id = random.choice(
            customer_ids
        )

        session_start = datetime.now()

        # Each session has multiple events
        num_events = random.randint(3, 10)

        selected_events = ["view_product"]

        # Build realistic funnel behavior
        if random.random() > 0.3:
            selected_events.append("add_to_cart")

        if random.random() > 0.5:
            selected_events.append("checkout")

        if random.random() > 0.6:
            selected_events.append("purchase")

        # Add extra browsing noise
        while len(selected_events) < num_events:
            selected_events.append(
                random.choice(EVENT_TYPES)
            )

        selected_events = sorted(
            selected_events,
            key=lambda x: EVENT_TYPES.index(x)
            if x in EVENT_TYPES else 0
        )

        for i, event_type in enumerate(selected_events):

            event_timestamp = (
                session_start +
                timedelta(seconds=i * random.randint(5, 60))
            )

            event = {
                "session_id": session_id,
                "customer_id": customer_id,
                "event_type": event_type,
                "product_id": random.choice(product_ids),
                "event_timestamp": event_timestamp.isoformat()
            }

            events.append(event)

    return events
