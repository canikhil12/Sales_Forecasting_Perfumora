from google.colab import drive
import json
drive.mount('/content/drive')
with open('/content/drive/MyDrive/Secret/shopify.json') as f:
    creds = json.load(f)
import requests
import pandas as pd
from sqlalchemy import create_engine




# Shopify API config
SHOP_NAME = creds["shop_name"]
ACCESS_TOKEN = creds["access_token"]
API_VERSION = creds.get("api_version", "2024-04")
BASE_URL = f"https://{SHOP_NAME}.myshopify.com/admin/api/{API_VERSION}/orders.json"
HEADERS = {
    "X-Shopify-Access-Token": ACCESS_TOKEN,
    "Content-Type": "application/json"
}

# 🔄 Fetch Orders from Shopify
def fetch_orders(from_date, to_date):
    print(f" Fetching orders from {from_date} to {to_date}...")
    orders = []
    url = f"{BASE_URL}?status=any&created_at_min={from_date}&created_at_max={to_date}&limit=250"
    while url:
        response = requests.get(url, headers=HEADERS)
        if response.status_code != 200:
            print(f" Error: {response.status_code}")
            break
        data = response.json()
        batch = data.get("orders", [])
        print(f" Got {len(batch)} orders")
        orders.extend(batch)

        # Handle pagination
        link = response.headers.get("Link", "")
        next_url = None
        for part in link.split(","):
            if 'rel="next"' in part:
                next_url = part[part.find("<") + 1:part.find(">")]
        url = next_url
    return orders

# 💾 Save Orders to PostgreSQL
def save_orders_to_postgresql(orders):
    rows = []
    for order in orders:
        customer = order.get("customer", {})
        customer_name = f"{customer.get('first_name', '')} {customer.get('last_name', '')}".strip()
        for item in order.get("line_items", []):
            rows.append({
                "order_id": order.get("id"),
                "created_at": order.get("created_at"),
                "customer_name": customer_name,
                "product": item.get("name"),
                "sku": item.get("sku", ""),
                "quantity": item.get("quantity"),
                "price": item.get("price"),
                "total": order.get("total_price")
            })

    df = pd.DataFrame(rows)
    conn_string = "postgresql://neondb_owner:npg_4lvIfcDWR8gx@ep-little-cherry-a89bmbz9-pooler.eastus2.azure.neon.tech/neondb?sslmode=require"
    engine = create_engine(conn_string)
    df.to_sql("orders", engine, if_exists="append", index=False)
    print(f"✅ {len(df)} orders saved to PostgreSQL")

# 🔁 Run the Full Pipeline
if __name__ == "__main__":
    orders = fetch_orders("2024-01-01", "2024-01-15")
    print(f"📊 Total orders fetched: {len(orders)}")
    if orders:
        save_orders_to_postgresql(orders)
    else:
        print("⚠️ No orders to save.")