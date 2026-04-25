import json
import os
import random
import uuid
from datetime import datetime, timedelta, timezone

RAW_BASE_DIR = "/opt/spark/data/raw"
random.seed(42)

stores = ["Columbus", "Cleveland", "Cincinnati", "Dayton"]
statuses = ["CREATED", "CONFIRMED", "CANCELLED"]
payment_methods = ["CARD", "PAYPAL", "APPLE_PAY"]
payment_statuses = ["AUTHORIZED", "SETTLED", "FAILED"]
shipment_carriers = ["UPS", "FedEx", "USPS"]
shipment_statuses = ["LABEL_CREATED", "IN_TRANSIT", "DELIVERED", "LOST"]

products = [
    {"sku": "SKU-100", "name": "Coffee Beans", "category": "Grocery", "price": 14.99},
    {"sku": "SKU-101", "name": "Wireless Mouse", "category": "Electronics", "price": 29.99},
    {"sku": "SKU-102", "name": "Notebook", "category": "Office", "price": 4.99},
    {"sku": "SKU-103", "name": "Desk Lamp", "category": "Home", "price": 24.99},
    {"sku": "SKU-104", "name": "Water Bottle", "category": "Fitness", "price": 19.99},
]

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def random_ts(days_back=30):
    now = datetime.now(timezone.utc)
    dt = now - timedelta(
        days=random.randint(0, days_back),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )
    return dt.isoformat()

def make_orders(n=500):
    rows = []
    for _ in range(n):
        order_id = str(uuid.uuid4())
        customer_id = random.randint(1000, 1100)
        item_count = random.randint(1, 4)
        items = []
        subtotal = 0.0

        for _ in range(item_count):
            product = random.choice(products)
            qty = random.randint(1, 3)
            line_total = round(product["price"] * qty, 2)
            subtotal += line_total
            items.append({
                "sku": product["sku"],
                "product_name": product["name"],
                "category": product["category"],
                "quantity": qty,
                "unit_price": product["price"],
                "line_total": line_total
            })

        tax = round(subtotal * 0.07, 2)
        shipping = round(random.choice([0, 4.99, 7.99, 12.99]), 2)
        grand_total = round(subtotal + tax + shipping, 2)

        rows.append({
            "order_id": order_id,
            "customer_id": customer_id,
            "order_ts": random_ts(),
            "store": random.choice(stores),
            "status": random.choice(statuses),
            "currency": "USD",
            "items": items,
            "subtotal": round(subtotal, 2),
            "tax": tax,
            "shipping_amount": shipping,
            "grand_total": grand_total
        })
    return rows

def make_payments(orders):
    rows = []
    for order in orders:
        rows.append({
            "payment_id": str(uuid.uuid4()),
            "order_id": order["order_id"],
            "payment_ts": random_ts(),
            "method": random.choice(payment_methods),
            "status": random.choices(payment_statuses, weights=[15, 75, 10])[0],
            "amount": order["grand_total"],
            "currency": "USD"
        })
    return rows

def make_shipments(orders):
    rows = []
    for order in orders:
        if order["status"] != "CANCELLED":
            rows.append({
                "shipment_id": str(uuid.uuid4()),
                "order_id": order["order_id"],
                "shipment_ts": random_ts(),
                "carrier": random.choice(shipment_carriers),
                "status": random.choices(shipment_statuses, weights=[10, 30, 55, 5])[0],
                "tracking_number": f"TRK{random.randint(10000000, 99999999)}"
            })
    return rows

def write_ndjson(path, rows):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")

if __name__ == "__main__":
    batch_id = datetime.now().strftime("%Y%m%d%H%M%S")

    orders = make_orders(500)
    payments = make_payments(orders)
    shipments = make_shipments(orders)

    write_ndjson(f"{RAW_BASE_DIR}/orders/orders_{batch_id}.json", orders)
    write_ndjson(f"{RAW_BASE_DIR}/payments/payments_{batch_id}.json", payments)
    write_ndjson(f"{RAW_BASE_DIR}/shipments/shipments_{batch_id}.json", shipments)

    print(f"Generated batch {batch_id}")
    print(f"Orders: {len(orders)}")
    print(f"Payments: {len(payments)}")
    print(f"Shipments: {len(shipments)}")
