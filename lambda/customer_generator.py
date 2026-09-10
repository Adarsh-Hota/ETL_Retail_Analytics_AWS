import random
import uuid

import pandas as pd

from faker import Faker
from datetime import datetime, timedelta

fake = Faker()


def generate_customers(num_records=100, run_id=None, ingested_at=None):

    loyalty_tiers = [
        "Bronze",
        "Silver",
        "Gold",
        "Platinum"
    ]

    PREFERRED_CATEGORIES = [
        "Electronics",
        "Fashion",
        "Home",
        "Sports",
        "Books"
    ]

    customers = []

    for _ in range(num_records):

        signup_date = (
            datetime.now() -
            timedelta(days=random.randint(1, 1095))
        ).date()

        source_updated_at = datetime.utcnow().isoformat()
        customer = {
            "source_event_id": str(uuid.uuid4()),

            "customer_id": f"CUST_{uuid.uuid4().hex[:8].upper()}",

            "operation": "I",

            "source_updated_at": source_updated_at,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "signup_date": signup_date.isoformat(),
            "loyalty_tier": random.choices(
                loyalty_tiers,
                weights=[50, 30, 15, 5]
            )[0],
            "preferred_category": random.choice(
                PREFERRED_CATEGORIES
            ),

            "ingested_at": ingested_at or source_updated_at,

            "run_id": run_id or str(uuid.uuid4()),
        }

        customers.append(customer)

    return pd.DataFrame(customers)
