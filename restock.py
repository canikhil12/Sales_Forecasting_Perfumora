import pandas as pd
from sqlalchemy import create_engine

def generate_restock_report_from_neon():
    # PostgreSQL connection
    conn_str = "postgresql://neondb_owner:npg_4lvIfcDWR8gx@ep-little-cherry-a89bmbz9-pooler.eastus2.azure.neon.tech/neondb?sslmode=require"
    engine = create_engine(conn_str)

    # 📥 Load order data (sales)
    orders_df = pd.read_sql("SELECT sku, quantity, created_at FROM orders", engine)
    orders_df["created_at"] = pd.to_datetime(orders_df["created_at"])
    orders_df["date"] = orders_df["created_at"].dt.date
    orders_df["quantity"] = pd.to_numeric(orders_df["quantity"], errors="coerce").fillna(0)

    # 📥 Load my_stock and rename correctly
    my_stock_df = pd.read_sql("SELECT * FROM my_stock", engine)
    my_stock_df["my_stock_qty"] = pd.to_numeric(my_stock_df["Quantity On Hand"], errors="coerce").fillna(0)

    # 📥 Load supplier_master and rename correctly
    supplier_df = pd.read_sql("SELECT * FROM supplier_master", engine)
    supplier_df["supplier_stock_qty"] = pd.to_numeric(supplier_df["AvailableQuantity"], errors="coerce").fillna(0)

    # 📊 Calculate avg daily sales per SKU
    summary = (
        orders_df.groupby(["sku", "date"])["quantity"].sum()
        .groupby("sku")
        .agg(avg_daily_sales="mean", days_sold="count")
        .reset_index()
    )

    # 🔁 Merge sales summary with my_stock
    stock_summary = pd.merge(my_stock_df, summary, on="sku", how="left").fillna({"avg_daily_sales": 0})
    stock_summary["days_coverage"] = stock_summary["my_stock_qty"] / stock_summary["avg_daily_sales"].replace(0, 0.01)
    stock_summary["restock_needed"] = stock_summary["days_coverage"] < 15

    # 🔁 Join only restock-needed items with supplier data
    restock_df = pd.merge(stock_summary[stock_summary["restock_needed"] == True], supplier_df, on="sku", how="left")

    # 🧾 Upload result to Neon
    restock_df.to_sql("restock_recommendations", engine, if_exists="replace", index=False)
    print("✅ Restock recommendations uploaded to Neon as 'restock_recommendations' table.")

# ✅ Execute
generate_restock_report_from_neon()




