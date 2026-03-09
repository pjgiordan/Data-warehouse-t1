import pyodbc
import csv

# Connect to dwh
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

# Define tables and their CSV file paths
tables = [
    {
        "table": "ingestion.crm_cust_info",
        "file": "/Users/mac2024/Downloads/datasets.DataEngineering/source_crm/cust_info.csv",
        "columns": 7
    },
    {
        "table": "ingestion.crm_prd_info",
        "file": "/Users/mac2024/Downloads/datasets.DataEngineering/source_crm/prd_info.csv",
        "columns": 7
    },
    {
        "table": "ingestion.crm_sales_details",
        "file": "/Users/mac2024/Downloads/datasets.DataEngineering/source_crm/sales_details.csv",
        "columns": 9
    },
    {
        "table": "ingestion.erp_cust_az12",
        "file": "/Users/mac2024/Downloads/datasets.DataEngineering/source_erp/CUST_AZ12.csv",
        "columns": 3
    },
    {
        "table": "ingestion.erp_loc_a101",
        "file": "/Users/mac2024/Downloads/datasets.DataEngineering/source_erp/LOC_A101.csv",
        "columns": 2
    },
    {
        "table": "ingestion.erp_px_cat_g1v2",
        "file": "/Users/mac2024/Downloads/datasets.DataEngineering/source_erp/PX_CAT_G1V2.csv",
        "columns": 4
    },
]

for t in tables:
    table_name = t["table"]
    file_path = t["file"]
    placeholders = ",".join(["?" for _ in range(t["columns"])])

    # Truncate table before loading
    cursor.execute(f"TRUNCATE TABLE {table_name}")

    # Read CSV and insert rows
    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)  # Skip header row
        rows = 0
        for row in reader:
            row = [None if val == "" else val for val in row]
            cursor.execute(f"INSERT INTO {table_name} VALUES ({placeholders})", row)
            rows += 1

    print(f"Loaded {rows} rows into {table_name}")

cursor.close()
conn.close()
print("All tables loaded!")