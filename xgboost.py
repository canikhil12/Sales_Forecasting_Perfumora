import pandas as pd, numpy as np, os
from xgboost import XGBRegressor
from sqlalchemy import create_engine, text
from sklearn.model_selection import train_test_split
def XGboost():
    # ─── CONFIG ──────────────────────────────────────────────
    NEON_URL          = "postgresql://neondb_owner:npg_4lvIfcDWR8gx@ep-little-cherry-a89bmbz9-pooler.eastus2.azure.neon.tech/neondb?sslmode=require"
    FORECAST_HORIZON  = 30
    VELOCITY_DAYS     = 30
    FAST_QTL          = 0.60     # ← relaxed
    TREND_THRESHOLD   = 0.60     # ← relaxed
    MARGIN_PCT        = 0.15     # ← relaxed
    SAFETY_DAYS       = 7
    MIN_DAYS_SKU      = 3        # ← relaxed
    SUPPLIER_CHEAPER  = True     # set False to skip that filter

    engine = create_engine(NEON_URL)

    # ─── 1. LOAD TABLES ─────────────────────────────────────
    orders = pd.read_sql(text("SELECT sku, quantity, created_at FROM orders"), engine)
    trend  = pd.read_sql(text('''
        SELECT sku, "TrendScore" AS trendscore, avg_market_price FROM trend_data
    '''), engine)
    supplier_raw = pd.read_sql(text('''
        SELECT sku, "AvailableQuantity" AS availablequantity, "UnitCost" AS unitcost
        FROM supplier_master
    '''), engine)
    mystock = pd.read_sql(text('''
        SELECT sku, "Quantity On Hand" AS on_hand FROM my_stock
    '''), engine)

    for df in (orders, trend, supplier_raw, mystock):
        df.columns = df.columns.str.lower()

    # cheapest supplier row
    supplier = (supplier_raw.sort_values("unitcost")
                           .drop_duplicates("sku", keep="first"))

    # ─── 2. DAILY SALES + VELOCITY ──────────────────────────
    orders["created_at"] = pd.to_datetime(orders["created_at"])
    orders["quantity"]   = pd.to_numeric(orders["quantity"], errors="coerce").fillna(0)

    daily = (orders.groupby(["sku", orders["created_at"].dt.date])
                    .agg(quantity=("quantity", "sum"))
                    .reset_index()
                    .rename(columns={"created_at": "ds", "quantity": "y"}))

    recent = orders.loc[orders["created_at"] >=
                        orders["created_at"].max() - pd.Timedelta(days=VELOCITY_DAYS)]
    velocity = (recent.groupby("sku")["quantity"].sum() / VELOCITY_DAYS
               ).rename("units_per_day").reset_index()
    velocity_cut = velocity["units_per_day"].quantile(FAST_QTL)

    # ─── 3. PER-SKU XGBoost ─────────────────────────────────
    fc_frames = []
    for sku in daily["sku"].unique():
        sku_df = daily[daily["sku"] == sku].copy()
        if len(sku_df) < MIN_DAYS_SKU:
            continue

        sku_df.sort_values("ds", inplace=True)
        sku_df["ds"]  = pd.to_datetime(sku_df["ds"])
        sku_df["dow"] = sku_df["ds"].dt.dayofweek
        sku_df["day"] = sku_df["ds"].dt.day
        sku_df["mon"] = sku_df["ds"].dt.month
        sku_df["lag1"] = sku_df["y"].shift(1)
        sku_df["lag7"] = sku_df["y"].shift(7)
        sku_df.fillna(method="ffill", inplace=True)   # keep rows
        if len(sku_df) < 3:
            continue

        X = sku_df[["dow", "day", "mon", "lag1", "lag7"]]
        y = sku_df["y"]
        x_tr, x_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, shuffle=False)

        model = XGBRegressor(n_estimators=150, learning_rate=0.1,
                             objective="reg:squarederror", tree_method="hist")
        model.fit(x_tr, y_tr)

        future_dates = pd.date_range(sku_df["ds"].max() + pd.Timedelta(days=1),
                                     periods=FORECAST_HORIZON)
        future = pd.DataFrame({
            "ds": future_dates,
            "dow": future_dates.dayofweek,
            "day": future_dates.day,
            "mon": future_dates.month,
            "lag1": y.values[-1],
            "lag7": y.values[-7] if len(y) >= 7 else y.values[-1],
        })
        future["yhat"] = np.maximum(
            model.predict(future[["dow", "day", "mon", "lag1", "lag7"]]), 0
        )
        future["sku"] = sku
        fc_frames.append(future)

    if not fc_frames:
        raise RuntimeError("No SKUs had enough history for forecasting")

    forecast = (pd.concat(fc_frames)
                  .rename(columns={"ds": "forecast_date", "yhat": "predicted_qty"}))

    # ─── 4. MERGE & OPTIONAL CHEAPER FILTER ─────────────────
    next30 = forecast.groupby("sku")["predicted_qty"].sum().rename("forecast_30d").reset_index()
    df = (next30.merge(trend, on="sku", how="left")
                .merge(supplier, on="sku", how="inner")
                .merge(mystock,  on="sku", how="left")
                .merge(velocity, on="sku", how="left"))

    df.fillna({
        "trendscore": 0,
        "avg_market_price": np.nan,
        "availablequantity": 0,
        "on_hand": 0,
        "units_per_day": 0,
        "unitcost": np.inf,
    }, inplace=True)

    df["avg_market_price"] = df["avg_market_price"].fillna(df["unitcost"] * 1.2)

    if SUPPLIER_CHEAPER:
        df = df[df["unitcost"] < df["avg_market_price"]]

    # ─── 5. FLAGS, BUY_QTY, REASON ──────────────────────────
    df["gross_margin"] = df["avg_market_price"] - df["unitcost"]
    df["margin_pct"]   = df["gross_margin"] / df["avg_market_price"].replace(0, np.nan)
    df["fast_tag"]   = (df["units_per_day"] >= velocity_cut) | (df["trendscore"] >= TREND_THRESHOLD)
    df["margin_tag"] = df["margin_pct"] >= MARGIN_PCT
    df = df[df["fast_tag"] | df["margin_tag"]].copy()

    df["safety_stock"] = df["units_per_day"] * SAFETY_DAYS
    df["buy_qty"] = (
        (df["forecast_30d"] + df["safety_stock"]) - df["on_hand"]
    ).clip(lower=0).round().where(df["availablequantity"] > 0, 0)

    df["recommend_reason"] = np.select(
        [df["fast_tag"] & df["margin_tag"],
         df["fast_tag"],
         df["margin_tag"]],
        ["FAST & HIGH_MARGIN", "FAST_SELLER", "HIGH_MARGIN"],
        default="OTHER"
    )

    df["created_at"] = pd.Timestamp.utcnow()

    # ─── 6. SAVE ────────────────────────────────────────────
    df.to_sql("smart_forecast_xgb", engine, if_exists="replace", index=False, method="multi")
    df.to_csv("smart_forecast_xgb.csv", index=False)

    print(f"✅ {len(df)} SKUs saved to 'smart_forecast_xgb' and CSV generated.")