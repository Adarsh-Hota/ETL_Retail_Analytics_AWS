import random
import uuid

import pandas as pd

from datetime import datetime, timedelta


MOVEMENT_TYPES = [
    "STOCK_IN",
    "STOCK_OUT",
    "ADJUSTMENT"
]

WAREHOUSES = [
    "WH_BLR",
    "WH_MUM",
    "WH_DEL",
    "WH_HYD"
]


def generate_inventory_events(product_ids, num_records=100):

    inventory_events = []

    for _ in range(num_records):

        movement_type = random.choices(
            MOVEMENT_TYPES,
            weights=[30, 60, 10]
        )[0]

        if movement_type == "STOCK_IN":
            quantity_change = random.randint(10, 200)

        elif movement_type == "STOCK_OUT":
            quantity_change = -random.randint(1, 20)

        else:
            quantity_change = random.randint(-10, 10)

        inventory_event = {
            "inventory_event_id": (
                f"INV_{uuid.uuid4().hex[:8].upper()}"
            ),
            "product_id": random.choice(
                product_ids
            ),
            "warehouse_id": random.choice(
                WAREHOUSES
            ),
            "movement_type": movement_type,
            "quantity_change": quantity_change,
            "event_timestamp": (
                datetime.now()
                -
                timedelta(
                    hours=random.randint(
                        1,
                        720
                    )
                )
            ).isoformat()
        }

        inventory_events.append(
            inventory_event
        )

    return pd.DataFrame(
        inventory_events
    )
