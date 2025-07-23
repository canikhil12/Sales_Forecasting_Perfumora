# Sales_Forecasting_Perfumora

# 📊 Sales Forecasting & Inventory Optimization Pipeline
https://lookerstudio.google.com/reporting/9a9fa43a-bfe9-4e77-8031-f25a44ea7302
This project is an end-to-end data analytics pipeline designed to automate sales forecasting, inventory restocking, and sourcing decisions. It leverages Python, machine learning, PostgreSQL (Neon), Google Sheets, and Looker Studio to streamline retail operations.

---

## ❓ Business Problem

Retail businesses often struggle with:
- Stockouts and overstocking
- Manual, error-prone restocking processes
- Lack of predictive analytics for demand forecasting
- Poor visibility into future inventory and sourcing needs

### 📌 Core Questions:
1. Can we automate the process of sales forecasting and inventory planning?
2. Can we deliver accurate predictions in a business-friendly format?
3. Can we build a real-time dashboard for better decision-making?

---

## ✅ Solution Overview

We implemented a modular pipeline that:

- Loads raw sales data from Shopify or CSV
- Stores it in a cloud PostgreSQL database (Neon)
- Trains ML models (LightGBM, XGBoost) to forecast sales
- Generates restock and sourcing recommendations
- Pushes final outputs to Google Sheets
- Visualizes results in Looker Studio dashboard

---

## 🛠️ Technologies Used

- **Python** (Pandas, NumPy, LightGBM, XGBoost)
- **PostgreSQL (Neon Cloud)**
- **Google Colab** for execution
- **Google Sheets API** for output
- **Looker Studio** for dashboard
- **Git & GitHub** for version control


