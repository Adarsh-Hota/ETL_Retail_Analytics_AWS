import random
import uuid

import pandas as pd

from datetime import datetime, timedelta


CATEGORY_MAPPING = {
    "Electronics": [
        "Phones",
        "Laptops",
        "Accessories"
    ],
    "Clothing": [
        "Men",
        "Women",
        "Kids"
    ],
    "Home": [
        "Furniture",
        "Kitchen",
        "Decor"
    ],
    "Sports": [
        "Fitness",
        "Outdoor",
        "Cycling"
    ],
    "Books": [
        "Fiction",
        "Non-Fiction",
        "Education"
    ],
    "Beauty": [
        "Skincare",
        "Makeup",
        "Haircare"
    ]
}


BRANDS = [
    "TechPro",
    "UrbanStyle",
    "HomeEase",
    "FitLife",
    "ReadMore",
    "GlowCare"
]


def generate_products(num_records=100, run_id=None, ingested_at=None):

    products = []

    for _ in range(num_records):

        category = random.choice(
            list(CATEGORY_MAPPING.keys())
        )

        subcategory = random.choice(
            CATEGORY_MAPPING[category]
        )

        cost_price = round(
            random.uniform(100, 5000),
            2
        )

        price = round(
            cost_price * random.uniform(1.2, 2.0),
            2
        )

        popularity_score = random.randint(1, 100)

        source_updated_at = datetime.utcnow().isoformat()
        product = {
            "source_event_id": str(uuid.uuid4()),

            "product_id": (
                f"PROD_{uuid.uuid4().hex[:8].upper()}"
            ),

            "operation": "I",

            "source_updated_at": source_updated_at,

            "product_name": (
                f"{subcategory} Product "
                f"{random.randint(1000,9999)}"
            ),

            "category": category,

            "subcategory": subcategory,

            "brand": random.choice(
                BRANDS
            ),

            "cost_price": cost_price,

            "price": price,

            "popularity_score": popularity_score,

            "launch_date": (
                datetime.now()
                -
                timedelta(
                    days=random.randint(
                        1,
                        1825
                    )
                )
            ).date().isoformat(),

            "is_active": random.choice(
                [True, True, True, False]
            ),

            "ingested_at": ingested_at or source_updated_at,

            "run_id": run_id or str(uuid.uuid4()),
        }

        products.append(product)

    return pd.DataFrame(products)
