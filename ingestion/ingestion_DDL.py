#paul carl giordanelli's code - Ingestion_DDL
import pyodbc

conn = pyodbc.connect(
    "DRIVER={PostgreSQL Unicode};"
    "SERVER=localhost;"
    "PORT=5432;"
    "DATABASE=postgres;"
    "UID=postgres;"
    "PWD=Figaro123;"
)
conn.autocommit = True
cursor = conn.cursor()

# Check if database exists
cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'dwh'")
if not cursor.fetchone():
    cursor.execute("CREATE DATABASE dwh")
    print("Database 'dwh' created!")
else:
    print("Database 'dwh' already exists.")

conn.commit()
cursor.close()
conn.close()

#connect to dwh
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

# Create schema if it doesn't exist
cursor.execute("CREATE SCHEMA IF NOT EXISTS ingestion")
print("Schema created!")

# Drop and create table
cursor.execute("DROP TABLE IF EXISTS ingestion.crm_cust_info")
cursor.execute("""
    CREATE TABLE ingestion.crm_cust_info (
        cst_id INTEGER,
        cst_key VARCHAR(50),
        cst_firstname VARCHAR(50),
        cst_lastname VARCHAR(50),
        cst_marital_status VARCHAR(50),
        cst_gndr VARCHAR(50),
        cst_create_date DATE
    )
""")
print("Table ingestion.crm_cust_info created!")

# crm_prd_info
cursor.execute("DROP TABLE IF EXISTS ingestion.crm_prd_info")
cursor.execute("""
    CREATE TABLE ingestion.crm_prd_info (
        prd_id INTEGER,
        prd_key VARCHAR(50),
        prd_nm VARCHAR(50),
        prd_cost INTEGER,
        prd_line VARCHAR(50),
        prd_start_dt DATE,
        prd_end_dt DATE
    )
""")
print("Table ingestion.crm_prd_info created!")

# crm_sales_details
cursor.execute("DROP TABLE IF EXISTS ingestion.crm_sales_details")
cursor.execute("""
    CREATE TABLE ingestion.crm_sales_details (
        sls_ord_num VARCHAR(50),
        sls_prd_key VARCHAR(50),
        sls_cust_id INTEGER,
        sls_order_dt INTEGER,
        sls_ship_dt INTEGER,
        sls_due_dt INTEGER,
        sls_sales DECIMAL(10,2),
        sls_quantity INTEGER,
        sls_price DECIMAL(10,2)
    )
""")
print("Table ingestion.crm_sales_details created!")

# erp_CUST_AZ12
cursor.execute("DROP TABLE IF EXISTS ingestion.erp_cust_az12")
cursor.execute("""
    CREATE TABLE ingestion.erp_cust_az12 (
        cid VARCHAR(50),
        bdate DATE,
        gen VARCHAR(50)
    )
""")
print("Table ingestion.erp_cust_az12 created!")

# erp_LOC_A101
cursor.execute("DROP TABLE IF EXISTS ingestion.erp_loc_a101")
cursor.execute("""
    CREATE TABLE ingestion.erp_loc_a101 (
        cid VARCHAR(50),
        cntry VARCHAR(50)
    )
""")
print("Table ingestion.erp_loc_a101 created!")

# erp_PX_CAT_G1V2
cursor.execute("DROP TABLE IF EXISTS ingestion.erp_px_cat_g1v2")
cursor.execute("""
    CREATE TABLE ingestion.erp_px_cat_g1v2 (
        id VARCHAR(50),
        cat VARCHAR(50),
        subcat VARCHAR(50),
        maintenance VARCHAR(50)
    )
""")
print("Table ingestion.erp_px_cat_g1v2 created!")


cursor.close()
conn.close()



