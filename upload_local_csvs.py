import os
import pandas as pd
from sqlalchemy import create_engine

def run_upload():
# Neon PostgreSQL connection
  conn_str = "postgresql://neondb_owner:npg_4lvIfcDWR8gx@ep-little-cherry-a89bmbz9-pooler.eastus2.azure.neon.tech/neondb?sslmode=require"
  engine = create_engine(conn_str)

# Folder paths
  supplier_folder = '/content/drive/MyDrive/Sales_Forecasting/Data/supplier_lists'
  my_stock_folder = '/content/drive/MyDrive/Sales_Forecasting/Data/my_stock'
  trend_data_folder='/content/drive/MyDrive/Sales_Forecasting/Data/trend_data'

# --- Step 1: Upload supplier data ---
  supplier_files = [f for f in os.listdir(supplier_folder) if f.endswith(".csv")]
  supplier_data = []

  for file in supplier_files:
      file_path = os.path.join(supplier_folder, file)
      df = pd.read_csv(file_path)
      supplier_name = os.path.splitext(file)[0]
      df["supplier_name"] = supplier_name
      df["source"] = "supplier"
      supplier_data.append(df)

  supplier_df = pd.concat(supplier_data, ignore_index=True)
  supplier_df.to_sql("supplier_master", engine, if_exists="replace", index=False)
  print("✅ Uploaded supplier data to 'supplier_master' table.")

# --- Step 2: Upload my_stock data ---
  stock_files = [f for f in os.listdir(my_stock_folder) if f.endswith(".csv")]
  stock_data = []

  for file in stock_files:
      file_path = os.path.join(my_stock_folder, file)
      df = pd.read_csv(file_path)
      stock_source = os.path.splitext(file)[0]
      df["stock_source"] = stock_source
      df["source"] = "my_stock"
      stock_data.append(df)

  stock_df = pd.concat(stock_data, ignore_index=True)
  stock_df.to_sql("my_stock", engine, if_exists="replace", index=False)
  print("✅ Uploaded stock data to 'my_stock' table.")

  trend_files = [f for f in os.listdir(trend_data_folder) if f.endswith(".csv")]
  trend_data = []
  for file in trend_files:
      file_path = os.path.join(trend_data_folder, file)
      df = pd.read_csv(file_path)
      trend_source = os.path.splitext(file)[0]
      df["trend_source"] = trend_source
      df["source"] = "trend_data"
      stock_data.append(df)

  stock_df = pd.concat(stock_data, ignore_index=True)
  stock_df.to_sql("trend_data", engine, if_exists="replace", index=False)
  print("✅ Uploaded trend data to 'trend_data' table.")