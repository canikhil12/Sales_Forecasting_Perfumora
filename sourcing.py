import pandas as pd
from sqlalchemy import create_engine

def generate_sourcing_recommendation_from_neon():
    # 🔌 Connect to Neon PostgreSQL
    db_uri = "postgresql://neondb_owner:npg_4lvIfcDWR8gx@ep-little-cherry-a89bmbz9-pooler.eastus2.azure.neon.tech/neondb?sslmode=require"
    engine = create_engine(db_uri)

    # 📥 Load trend data from Neon
    trend_df = pd.read_sql("SELECT * FROM trend_data", engine)

    # 📥 Load supplier data from Neon
    supplier_df = pd.read_sql("SELECT * FROM supplier_master", engine)

    # 🔗 Merge by SKU
    merged_df = pd.merge(trend_df, supplier_df, on="sku", how="inner")

    # 💰 Calculate potential margin
    merged_df["potential_margin"] = merged_df["avg_market_price"] - merged_df["UnitCost"]

    # 🎯 Filter high margin & popularity
    filtered = merged_df[
        (merged_df["potential_margin"] > 10) &
        (merged_df["TrendScore"] >= 0.75)
    ].sort_values(by=["potential_margin", "TrendScore"], ascending=False)

    # 💾 Save recommendations back to Neon
    filtered.to_sql("sourcing_recommendations", engine, if_exists="replace", index=False)

    print("✅ Sourcing recommendations saved to Neon as 'sourcing_recommendations'")

# ✅ Run the function
generate_sourcing_recommendation_from_neon()

