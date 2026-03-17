import pyodbc
import pandas as pd

conn = pyodbc.connect(
    "DRIVER={PostgreSQL Unicode};"
    "SERVER=localhost;"
    "PORT=5432;"
    "DATABASE=dwh;"
    "UID=postgres;"
    "PWD=Figaro123;"
)
conn.autocommit = True
cursor = conn.cursor()

# Create transformation schema if it doesn't exist
cursor.execute("CREATE SCHEMA IF NOT EXISTS transformation")

# Read from ingestion layer
df = pd.read_sql_query(sql="SELECT * FROM ingestion.crm_sales_details", con=conn)

# Convert sls_ship_dt and sls_due_dt to datetime first (needed as reference for fallback)
df["sls_ship_dt"] = pd.to_datetime(df["sls_ship_dt"].astype(str), format="%Y%m%d", errors="coerce")
df["sls_due_dt"] = pd.to_datetime(df["sls_due_dt"].astype(str), format="%Y%m%d", errors="coerce")

# Fix sls_order_dt while still as integers:
# Valid = exactly 8 digits (YYYYMMDD); invalid = 0, too short (e.g. 32154, 5489)
# - If group has at least one valid date: replace invalids with the max valid integer in the group
# - If all invalid: replace with sls_ship_dt - 1 day (stored back as YYYYMMDD integer)
def fix_order_dt(group):
    valid_mask = group["sls_order_dt"].astype(str).str.len() == 8
    valid_vals = group.loc[valid_mask, "sls_order_dt"]
    if valid_vals.empty:
        fixed = df.loc[group.index, "sls_ship_dt"] - pd.Timedelta(days=1)
        group["sls_order_dt"] = fixed.dt.strftime("%Y%m%d").astype(int)
    else:
        max_valid = valid_vals.max()
        group.loc[~valid_mask, "sls_order_dt"] = max_valid
    return group

df = df.groupby("sls_ord_num", group_keys=False).apply(fix_order_dt)

# Now convert sls_order_dt to datetime
df["sls_order_dt"] = pd.to_datetime(df["sls_order_dt"].astype(str), format="%Y%m%d", errors="coerce")

# Fix sls_sales and sls_price (price is the source of truth):
# 1. Both 0 → null
both_zero = (df["sls_sales"] == 0) & (df["sls_price"] == 0)
df.loc[both_zero, ["sls_sales", "sls_price"]] = None

# 2. Price is invalid (null, zero, or negative), sales is valid → price = sales / quantity
invalid_price = df["sls_price"].isna() | (df["sls_price"] <= 0)
valid_sales = df["sls_sales"].notna() & (df["sls_sales"] > 0)
df.loc[invalid_price & valid_sales, "sls_price"] = (
    df.loc[invalid_price & valid_sales, "sls_sales"] / df.loc[invalid_price & valid_sales, "sls_quantity"]
)

# 3. Sales is invalid (null, zero, or negative), price is valid → sales = price * quantity
invalid_sales = df["sls_sales"].isna() | (df["sls_sales"] <= 0)
valid_price = df["sls_price"].notna() & (df["sls_price"] > 0)
df.loc[invalid_sales & valid_price, "sls_sales"] = (
    df.loc[invalid_sales & valid_price, "sls_price"] * df.loc[invalid_sales & valid_price, "sls_quantity"]
)

# 4. Sales == price but quantity > 1 → price is correct, recalculate sales
wrong_sales = (df["sls_sales"] == df["sls_price"]) & (df["sls_quantity"] > 1)
df.loc[wrong_sales, "sls_sales"] = df.loc[wrong_sales, "sls_price"] * df.loc[wrong_sales, "sls_quantity"]

# Drop and recreate the table in transformation schema
cursor.execute("DROP TABLE IF EXISTS transformation.crm_sales_details")
cursor.execute("""
    CREATE TABLE transformation.crm_sales_details (
        sls_ord_num VARCHAR(50),
        sls_prd_key VARCHAR(50),
        sls_cust_id INTEGER,
        sls_order_dt DATE,
        sls_ship_dt DATE,
        sls_due_dt DATE,
        sls_sales NUMERIC(10,2),
        sls_quantity INTEGER,
        sls_price NUMERIC(10,2)
    )
""")

# Insert cleaned data into transformation layer
placeholders = ",".join(["?" for _ in range(len(df.columns))])
for _, row in df.iterrows():
    values = [None if pd.isna(v) else v for v in row]
    cursor.execute(f"INSERT INTO transformation.crm_sales_details VALUES ({placeholders})", values)

print(f"Loaded {len(df)} cleaned rows into transformation.crm_sales_details")

cursor.close()
conn.close()
