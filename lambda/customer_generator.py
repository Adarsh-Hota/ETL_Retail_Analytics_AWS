import random
import uuid

import pandas as pd

from faker import Faker
from datetime import datetime, timedelta

fake = Faker()


def generate_customers(num_records=100):

    loyalty_tiers = [
        "Bronze",
        "Silver",
        "Gold",
        "Platinum"
    ]

    customers = []

    for _ in range(num_records):

        signup_date = (
            datetime.now() -
            timedelta(days=random.randint(1, 1095))
        ).date()

        customer = {
            "customer_id": f"CUST_{uuid.uuid4().hex[:8].upper()}",
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "signup_date": signup_date.isoformat(),
            "loyalty_tier": random.choices(
                loyalty_tiers,
                weights=[50, 30, 15, 5]
            )[0]
        }

        customers.append(customer)

    return pd.DataFrame(customers)
