import pandas as pd
from sqlalchemy import create_engine

def generate_final_inventory_actions_from_neon():
    # PostgreSQL connection string (Neon)
    conn_str = "postgresql://neondb_owner:npg_4lvIfcDWR8gx@ep-little-cherry-a89bmbz9-pooler.eastus2.azure.neon.tech/neondb?sslmode=require"
    engine = create_engine(conn_str)

    # 📥 Load restock and sourcing recommendations from Neon
    restock_df = pd.read_sql("SELECT * FROM restock_recommendations", engine)
    sourcing_df = pd.read_sql("SELECT * FROM sourcing_recommendations", engine)

    # ✅ Add action labels
    restock_df['action'] = 'Restock'
    sourcing_df['action'] = 'Source New'

    # ✅ Normalize columns for consistency
    restock_df = restock_df.rename(columns={
        'available_stock': 'stock_available',
        'avg_daily_sales': 'daily_sales_rate',
        'product_name': 'product_name'  # ensure it's named consistently
    })

    sourcing_df = sourcing_df.rename(columns={
        'available_stock': 'stock_available',
        'product_name': 'product_name'
    })

    # ✅ Columns to align
    restock_cols = ['sku', 'product_name', 'stock_available', 'daily_sales_rate', 'days_coverage', 'action']
    sourcing_cols = ['sku', 'product_name', 'stock_available', 'TrendScore', 'potential_margin', 'action']

    # Add missing columns with None to align both DataFrames
    for col in sourcing_cols:
        if col not in restock_df.columns:
            restock_df[col] = None
    for col in restock_cols:
        if col not in sourcing_df.columns:
            sourcing_df[col] = None

    # ✅ Combine both DataFrames
    combined_df = pd.concat([
        restock_df[restock_cols + list(set(sourcing_cols) - set(restock_cols))],
        sourcing_df[sourcing_cols + list(set(restock_cols) - set(sourcing_cols))]
    ], ignore_index=True)

    # ✅ Save combined actions to Neon
    combined_df.to_sql("final_inventory_actions", engine, if_exists="replace", index=False)

    print("✅ Final inventory actions saved to Neon as 'final_inventory_actions' table.")

# ▶️ Run it
generate_final_inventory_actions_from_neon()