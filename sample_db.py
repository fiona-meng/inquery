"""
sample_db.py — Generate a mock e-commerce SQLite database for demo purposes.
Run once on startup if the file does not exist.
"""

import random
import sqlite3
from datetime import date, timedelta
from pathlib import Path

SAMPLE_DB_PATH = Path(__file__).parent / "sample_ecommerce.sqlite"

# ── Seed data ─────────────────────────────────────────────────────────────────

FIRST_NAMES = [
    "Emma","Liam","Olivia","Noah","Ava","Ethan","Sophia","Mason","Isabella","Logan",
    "Mia","Lucas","Charlotte","Oliver","Amelia","Aiden","Harper","Elijah","Evelyn","James",
    "Abigail","Benjamin","Emily","Sebastian","Elizabeth","Matthew","Mila","Henry","Ella","Alexander",
    "Scarlett","Daniel","Grace","Jackson","Chloe","Michael","Victoria","Owen","Riley","Samuel",
    "Aria","David","Lily","Joseph","Eleanor","Carter","Hannah","Wyatt","Lillian","John",
]

LAST_NAMES = [
    "Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis","Martinez","Wilson",
    "Anderson","Taylor","Thomas","Hernandez","Moore","Martin","Jackson","Thompson","White","Lopez",
    "Lee","Gonzalez","Harris","Clark","Lewis","Robinson","Walker","Perez","Hall","Young",
    "Allen","Sanchez","Wright","King","Scott","Green","Baker","Adams","Nelson","Carter",
    "Mitchell","Perez","Roberts","Turner","Phillips","Campbell","Parker","Evans","Edwards","Collins",
]

CITIES = [
    ("New York", "US"), ("Los Angeles", "US"), ("Chicago", "US"), ("Houston", "US"),
    ("Phoenix", "US"), ("San Francisco", "US"), ("Seattle", "US"), ("Austin", "US"),
    ("Boston", "US"), ("Miami", "US"), ("London", "GB"), ("Manchester", "GB"),
    ("Toronto", "CA"), ("Vancouver", "CA"), ("Sydney", "AU"), ("Melbourne", "AU"),
    ("Paris", "FR"), ("Berlin", "DE"), ("Amsterdam", "NL"), ("Singapore", "SG"),
]

PRODUCTS = [
    # (name, category, price, cost)
    ("Classic White Tee", "Clothing", 29.99, 8.00),
    ("Slim Fit Jeans", "Clothing", 79.99, 22.00),
    ("Floral Summer Dress", "Clothing", 64.99, 18.00),
    ("Wool Blend Sweater", "Clothing", 89.99, 28.00),
    ("Athletic Shorts", "Clothing", 39.99, 11.00),
    ("Linen Button-Down Shirt", "Clothing", 59.99, 16.00),
    ("Yoga Leggings", "Clothing", 54.99, 15.00),
    ("Puffer Jacket", "Clothing", 129.99, 42.00),
    ("Canvas Tote Bag", "Accessories", 34.99, 9.00),
    ("Leather Wallet", "Accessories", 49.99, 14.00),
    ("Minimalist Watch", "Accessories", 149.99, 45.00),
    ("Sunglasses", "Accessories", 69.99, 19.00),
    ("Knitted Beanie", "Accessories", 24.99, 7.00),
    ("Silk Scarf", "Accessories", 44.99, 13.00),
    ("Belt", "Accessories", 39.99, 11.00),
    ("Crossbody Bag", "Accessories", 89.99, 28.00),
    ("Moisturizer SPF 30", "Skincare", 38.99, 11.00),
    ("Vitamin C Serum", "Skincare", 52.99, 16.00),
    ("Hyaluronic Acid Toner", "Skincare", 34.99, 10.00),
    ("Retinol Night Cream", "Skincare", 64.99, 20.00),
    ("Gentle Cleanser", "Skincare", 28.99, 8.00),
    ("Eye Cream", "Skincare", 44.99, 14.00),
    ("Lip Balm Set", "Skincare", 19.99, 5.00),
    ("Face Mask Pack", "Skincare", 29.99, 8.00),
    ("Running Sneakers", "Footwear", 109.99, 36.00),
    ("Chelsea Boots", "Footwear", 139.99, 46.00),
    ("Slip-On Loafers", "Footwear", 79.99, 24.00),
    ("Sandals", "Footwear", 59.99, 17.00),
    ("High-Top Trainers", "Footwear", 119.99, 39.00),
    ("Ankle Boots", "Footwear", 129.99, 42.00),
]

ORDER_STATUSES = ["completed", "completed", "completed", "completed", "pending", "refunded"]


# ── Generator ─────────────────────────────────────────────────────────────────

def _rand_date(start: date, end: date, rng: random.Random) -> str:
    delta = (end - start).days
    return (start + timedelta(days=rng.randint(0, delta))).isoformat()


def create_sample_db(path: Path = SAMPLE_DB_PATH) -> Path:
    """Create the sample e-commerce SQLite database. Idempotent."""
    if path.exists():
        return path

    rng = random.Random(42)  # fixed seed → consistent data
    conn = sqlite3.connect(str(path))
    cur = conn.cursor()

    # ── Schema ─────────────────────────────────────────────────────────────────
    cur.executescript("""
        CREATE TABLE customers (
            id          INTEGER PRIMARY KEY,
            name        TEXT NOT NULL,
            email       TEXT NOT NULL UNIQUE,
            city        TEXT,
            country     TEXT,
            signup_date TEXT
        );

        CREATE TABLE products (
            id       INTEGER PRIMARY KEY,
            name     TEXT NOT NULL,
            category TEXT,
            price    REAL,
            cost     REAL
        );

        CREATE TABLE orders (
            id            INTEGER PRIMARY KEY,
            customer_id   INTEGER REFERENCES customers(id),
            order_date    TEXT,
            status        TEXT,
            total_amount  REAL
        );

        CREATE TABLE order_items (
            id          INTEGER PRIMARY KEY,
            order_id    INTEGER REFERENCES orders(id),
            product_id  INTEGER REFERENCES products(id),
            quantity    INTEGER,
            unit_price  REAL
        );
    """)

    # ── Customers (200) ────────────────────────────────────────────────────────
    customers = []
    used_emails = set()
    for i in range(1, 201):
        fn = rng.choice(FIRST_NAMES)
        ln = rng.choice(LAST_NAMES)
        base_email = f"{fn.lower()}.{ln.lower()}{rng.randint(1,999)}@example.com"
        while base_email in used_emails:
            base_email = f"{fn.lower()}.{ln.lower()}{rng.randint(1,9999)}@example.com"
        used_emails.add(base_email)
        city, country = rng.choice(CITIES)
        signup = _rand_date(date(2022, 1, 1), date(2023, 12, 31), rng)
        customers.append((i, f"{fn} {ln}", base_email, city, country, signup))

    cur.executemany(
        "INSERT INTO customers VALUES (?,?,?,?,?,?)", customers
    )

    # ── Products (30) ──────────────────────────────────────────────────────────
    for i, (name, category, price, cost) in enumerate(PRODUCTS, 1):
        cur.execute("INSERT INTO products VALUES (?,?,?,?,?)", (i, name, category, price, cost))

    # ── Orders + order_items ───────────────────────────────────────────────────
    order_id = 1
    item_id  = 1
    start_date = date(2023, 1, 1)
    end_date   = date(2024, 12, 31)

    for _ in range(500):
        cust_id    = rng.randint(1, 200)
        order_date = _rand_date(start_date, end_date, rng)
        status     = rng.choice(ORDER_STATUSES)
        n_items    = rng.randint(1, 5)

        total = 0.0
        items = []
        for _ in range(n_items):
            prod_id    = rng.randint(1, len(PRODUCTS))
            qty        = rng.randint(1, 3)
            unit_price = PRODUCTS[prod_id - 1][2]
            total     += qty * unit_price
            items.append((item_id, order_id, prod_id, qty, unit_price))
            item_id += 1

        if status == "refunded":
            total = -total

        cur.execute(
            "INSERT INTO orders VALUES (?,?,?,?,?)",
            (order_id, cust_id, order_date, status, round(total, 2))
        )
        cur.executemany("INSERT INTO order_items VALUES (?,?,?,?,?)", items)
        order_id += 1

    conn.commit()
    conn.close()
    return path


if __name__ == "__main__":
    p = create_sample_db()
    print(f"Created sample database: {p}")
