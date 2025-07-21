import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from lightgbm import LGBMRegressor
from sklearn.metrics import mean_absolute_error
import joblib
def LightBGM_sale():
# 📡 Neon PostgreSQL connection
  conn_str = "postgresql://neondb_owner:npg_4lvIfcDWR8gx@ep-little-cherry-a89bmbz9-pooler.eastus2.azure.neon.tech/neondb?sslmode=require"
  engine = create_engine(conn_str)
  supplier_df = pd.read_sql(
    'SELECT sku, "AvailableQuantity" AS availablequantity, "UnitCost" AS unitcost '
    'FROM supplier_master',
    engine
  )

# 📥 Load data from Neon
  orders_df = pd.read_sql("SELECT sku, quantity, created_at FROM orders", engine)
  
  #supplier.columns = supplier.columns.str.lower()     # → 'availablequantity'
  trend_df = pd.read_sql('SELECT sku, "TrendScore" AS trendscore, avg_market_price FROM trend_data', engine)

# 🧠 Merge supplier and trend data
  product_info_df = pd.merge(supplier_df, trend_df, on="sku", how="left")

# 🧹 Clean and prepare orders data
  orders_df['created_at'] = pd.to_datetime(orders_df['created_at'])
  orders_df['day_of_week'] = orders_df['created_at'].dt.dayofweek
  orders_df['day_of_month'] = orders_df['created_at'].dt.day
  orders_df['quantity'] = pd.to_numeric(orders_df['quantity'], errors='coerce').fillna(0)

# 📊 Aggregate sales per SKU and date
  agg_df = orders_df.groupby(['sku', 'created_at']).agg({'quantity': 'sum'}).reset_index()

# 🔁 Merge with product info (supplier + trend)
  merged_df = pd.merge(agg_df, product_info_df, on='sku', how='left')

# 🎯 Create 7-day rolling future target
  merged_df = merged_df.sort_values(['sku', 'created_at'])
  merged_df['target_sales'] = (
      merged_df.groupby('sku')['quantity']
      .shift(-1)
      .rolling(window=7)
      .sum()
      .reset_index(level=0, drop=True)
)
  merged_df = merged_df.dropna(subset=['target_sales'])

# 🧪 Feature engineering
  merged_df['day_of_week'] = merged_df['created_at'].dt.dayofweek
  merged_df['day_of_month'] = merged_df['created_at'].dt.day
  merged_df['availablequantity'] = merged_df['availablequantity'].fillna(0)
  merged_df['trendscore'] = merged_df['trendscore'].fillna(0)
  merged_df['avg_market_price'] = merged_df['avg_market_price'].fillna(merged_df['unitcost'] * 1.2)

# ✅ Define feature columns
  features = ['availablequantity', 'trendscore', 'avg_market_price', 'day_of_week', 'day_of_month']
  X = merged_df[features]
  y = merged_df['target_sales']

# 🔀 Train-test split
  X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
  model = LGBMRegressor()
  model.fit(X_train, y_train)

# 📈 Evaluate
  y_pred = model.predict(X_test)
  mae = mean_absolute_error(y_test, y_pred)
  print(f"✅ MAE: {mae:.2f}")

# 🧾 Prepare prediction DataFrame
  sample_output = X_test.copy()
  sample_output['actual_sales'] = y_test.values
  sample_output['predicted_sales'] = y_pred
  sample_output['sku'] = merged_df.loc[X_test.index, 'sku'].values
  sample_output['created_at'] = merged_df.loc[X_test.index, 'created_at'].values

# 📤 Save predictions to Neon PostgreSQL
  sample_output.to_sql("lgbm_forecast_output", engine, if_exists="replace", index=False)
  print("📊 Forecast saved to Neon table: 'lgbm_forecast_output'")