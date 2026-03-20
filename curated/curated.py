import pandas as pd
from sqlalchemy import create_engine, text

engine = create_engine("postgresql+psycopg2://postgres:Figaro123@localhost:5432/dwh")

# Create curated schema if it doesn't exist
with engine.connect() as con:
    con.execute(text("CREATE SCHEMA IF NOT EXISTS curated"))
    con.commit()

# -----------------------------------
# dim_customers
# -----------------------------------
customer_crm_df = pd.read_sql("SELECT * FROM transformation.crm_cust_info", con=engine)
customer_erp_df = pd.read_sql("SELECT * FROM transformation.erp_cust_az12", con=engine)
location_erp_df = pd.read_sql("SELECT * FROM transformation.erp_loc_a101", con=engine)

# Join CRM customers with ERP customers (bdate, gen)
df = pd.merge(
    left=customer_crm_df,
    right=customer_erp_df,
    how="left",
    left_on="cst_key",
    right_on="cid"
)

# Join with location (cntry)
df = pd.merge(
    left=df,
    right=location_erp_df,
    how="left",
    left_on="cst_key",
    right_on="cid",
    suffixes=("", "_loc")
)

dim_customers = pd.DataFrame({
    "customer_id": df["cst_id"],
    "customer_number": df["cst_key"],
    "first_name": df["cst_firstname"],
    "last_name": df["cst_lastname"],
    "country": df["cntry"],
    "marital_status": df["cst_marital_status"],
    "gender": df["cst_gndr"],
    "birthdate": df["bdate"],
    "create_date": df["cst_create_date"]
})

dim_customers = dim_customers.sort_values("customer_id").reset_index(drop=True)
dim_customers.insert(0, "customer_key", dim_customers.index + 1)

dim_customers.to_sql(
    name="dim_customers",
    con=engine,
    schema="curated",
    if_exists="replace",
    index=False
)

print(f"dim_customers loaded into curated.dim_customers ({len(dim_customers)} rows)")

# -----------------------------------
# dim_products
# -----------------------------------
product_crm_df = pd.read_sql("SELECT * FROM transformation.crm_prd_info", con=engine)
category_erp_df = pd.read_sql("SELECT * FROM ingestion.erp_px_cat_g1v2", con=engine)

# Join on prd_subcategory (crm) = id (erp)
df = pd.merge(
    left=product_crm_df,
    right=category_erp_df,
    how="left",
    left_on="prd_subcategory",
    right_on="id"
)

dim_products = pd.DataFrame({
    "product_number": df["prd_key"],
    "product_name": df["prd_nm"],
    "category_id": df["prd_subcategory"],
    "category": df["cat"],
    "subcategory": df["subcat"],
    "maintenance": df["maintenance"],
    "cost": df["prd_cost"],
    "product_line": df["prd_line"],
    "start_date": df["prd_start_dt"],
    "end_date": df["prd_end_dt"]
})

dim_products = dim_products.sort_values("product_number").reset_index(drop=True)
dim_products.insert(0, "product_key", dim_products.index + 1)

dim_products.to_sql(
    name="dim_products",
    con=engine,
    schema="curated",
    if_exists="replace",
    index=False
)

print(f"dim_products loaded into curated.dim_products ({len(dim_products)} rows)")

# -----------------------------------
# fact_sales
# -----------------------------------
sales_details_df = pd.read_sql("SELECT * FROM transformation.crm_sales_details", con=engine)
dim_products_df = pd.read_sql("SELECT * FROM curated.dim_products", con=engine)
dim_customers_df = pd.read_sql("SELECT * FROM curated.dim_customers", con=engine)

# Join sales to products
df = pd.merge(
    left=sales_details_df,
    right=dim_products_df[["product_key", "product_number"]],
    how="left",
    left_on="sls_prd_key",
    right_on="product_number"
)

# Join to customers
df = pd.merge(
    left=df,
    right=dim_customers_df[["customer_key", "customer_id"]],
    how="left",
    left_on="sls_cust_id",
    right_on="customer_id"
)

fact_sales = pd.DataFrame({
    "order_number": df["sls_ord_num"],
    "product_key": df["product_key"],
    "customer_key": df["customer_key"],
    "order_date": df["sls_order_dt"],
    "shipping_date": df["sls_ship_dt"],
    "due_date": df["sls_due_dt"],
    "sales": df["sls_sales"],
    "quantity": df["sls_quantity"],
    "price": df["sls_price"]
})

fact_sales = fact_sales.reset_index(drop=True)
fact_sales.insert(0, "sales_key", fact_sales.index + 1)

fact_sales.to_sql(
    name="fact_sales",
    con=engine,
    schema="curated",
    if_exists="replace",
    index=False
)

print(f"fact_sales loaded into curated.fact_sales ({len(fact_sales)} rows)")

