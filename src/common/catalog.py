"""
The one product catalogue, imported by the producer, the API and the UI.

Why this file exists: in the original version the producer emitted product IDs
9001-9005 while the dashboard emitted 101-105 (which were the producer's USER
ids). The two halves of the pipeline never shared a product space, so an event
from the producer could never render in the dashboard. Any ID that appears
anywhere in the system now comes from here.

`category` also gives the producer a reason to co-view certain products
together, so the co-occurrence pipeline has real signal to find rather
than uniform noise.
"""

import json
import os
import random
from pathlib import Path
from typing import Any

# The demo catalogue. Replaced at import time when CATALOG_FILE points at a
# catalogue built from a real dataset - see `load_file` below.
DEMO_PRODUCTS: list[dict] = [
    {"id": 9001, "name": "Apple MacBook Air M3", "price": 114900, "category": "laptop"},
    {"id": 9002, "name": "Dell XPS 13",          "price": 99990,  "category": "laptop"},
    {"id": 9003, "name": "Laptop Sleeve 13\"",   "price": 1499,   "category": "laptop-acc"},
    {"id": 9004, "name": "USB-C Hub 7-in-1",     "price": 2999,   "category": "laptop-acc"},
    {"id": 9005, "name": "Apple iPhone 16",      "price": 79900,  "category": "phone"},
    {"id": 9006, "name": "Samsung Galaxy S24",   "price": 74999,  "category": "phone"},
    {"id": 9007, "name": "Silicone Phone Case",  "price": 699,    "category": "phone-acc"},
    {"id": 9008, "name": "65W GaN Charger",      "price": 2499,   "category": "phone-acc"},
    {"id": 9009, "name": "Sony WH-1000XM5",      "price": 26990,  "category": "audio"},
    {"id": 9010, "name": "boAt Rockerz 550",     "price": 1999,   "category": "audio"},
    {"id": 9011, "name": "Adidas Ultraboost 22", "price": 16999,  "category": "footwear"},
    {"id": 9012, "name": "Puma Velocity Nitro",  "price": 8999,   "category": "footwear"},
]

USERS: list[int] = list(range(1001, 1051))       # 50 simulated shoppers

# Categories a shopper plausibly browses in the same session. This is what
# makes the pipeline's output checkable: if it works, laptops should
# surface laptop accessories, not footwear.
DEMO_AFFINITY: dict[str, list[str]] = {
    "laptop": ["laptop", "laptop-acc"],
    "laptop-acc": ["laptop-acc", "laptop"],
    "phone": ["phone", "phone-acc", "audio"],
    "phone-acc": ["phone-acc", "phone"],
    "audio": ["audio", "phone"],
    "footwear": ["footwear"],
}


def load_file(path: str) -> list[dict]:
    """A catalogue built from a real dataset, written by scripts/replay.py.

    Real items have no names or prices worth showing - RetailRocket hashes its
    item properties - so an entry is an id, a label and a category id. The
    shape is still validated, because a malformed catalogue would otherwise
    surface as a KeyError deep inside the API.
    """
    products = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(products, list) or not products:
        raise ValueError(f"{path} does not contain a non-empty list of products")
    for product in products:
        missing = {"id", "name", "category"} - set(product)
        if missing:
            raise ValueError(f"{path}: product {product} is missing {sorted(missing)}")
        product.setdefault("price", None)
    return products


# Which catalogue this process uses. Unset (the default) is the demo
# catalogue; set to a file, every part of the stack labels real items instead.
CATALOG_FILE = os.getenv("CATALOG_FILE", "").strip()

if CATALOG_FILE:
    PRODUCTS = load_file(CATALOG_FILE)
    # A real dataset has no hand-written affinity between categories: the only
    # thing known is which category an item belongs to, so a link is "related"
    # when both ends share one. The pipeline still has to discover the links
    # themselves from traffic.
    AFFINITY = {c: [c] for c in {p["category"] for p in PRODUCTS}}
else:
    PRODUCTS = DEMO_PRODUCTS
    AFFINITY = DEMO_AFFINITY

_BY_ID = {p["id"]: p for p in PRODUCTS}


def categories_related(a: str, b: str) -> bool:
    """True when the demo generator links the two categories directly."""
    return b in AFFINITY.get(a, []) or a in AFFINITY.get(b, [])


def product_ids() -> list[int]:
    return [p["id"] for p in PRODUCTS]


def get(product_id: int) -> dict | None:
    return _BY_ID.get(product_id)


def name_of(product_id: int) -> str:
    product = _BY_ID.get(product_id)
    return product["name"] if product else f"Unknown product {product_id}"


def is_demo() -> bool:
    """True when the built-in demo catalogue is in use."""
    return not CATALOG_FILE


def in_categories(categories: list[str]) -> list[dict]:
    wanted = set(categories)
    return [p for p in PRODUCTS if p["category"] in wanted]


def session_products(n: int, cross_category_rate: float,
                     rng: Any = random) -> list[dict]:
    """
    The products one shopper views in a single visit, in viewing order.

    The first product is random. Each further product comes from a related
    category (AFFINITY) - or, with probability `cross_category_rate`, from any
    category, because real browsing wanders. No product repeats in a visit;
    when the related categories run out, the visit simply ends early.

    This rule is the ONLY place the demo data's structure comes from: the
    streaming job knows nothing about categories and has to rediscover it.
    """
    first = rng.choice(PRODUCTS)
    related = in_categories(AFFINITY[first["category"]]) or PRODUCTS
    chosen = [first]
    for _ in range(max(n, 1) - 1):
        pool = PRODUCTS if rng.random() < cross_category_rate else related
        candidates = [p for p in pool if p not in chosen]
        if not candidates:
            break
        chosen.append(rng.choice(candidates))
    return chosen


def enrich(rows: list[dict], id_field: str = "product_id") -> list[dict]:
    """Attach name/price/category to API rows so the UI needs no lookup table."""
    out = []
    for row in rows:
        product = _BY_ID.get(row.get(id_field))
        out.append({**row,
                    "name": product["name"] if product else None,
                    "price": product["price"] if product else None,
                    "category": product["category"] if product else None})
    return out


def validate() -> None:
    ids = product_ids()
    if len(ids) != len(set(ids)):
        raise ValueError("Duplicate product ids in catalogue.")
    unknown = {p["category"] for p in PRODUCTS} - set(AFFINITY)
    if unknown:
        raise ValueError(f"Categories without an AFFINITY entry: {sorted(unknown)}")
    for category, related in AFFINITY.items():
        bad = set(related) - {p["category"] for p in PRODUCTS}
        if bad:
            raise ValueError(f"AFFINITY[{category}] references unknown {sorted(bad)}")


validate()
