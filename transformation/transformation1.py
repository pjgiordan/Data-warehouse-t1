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
df = pd.read_sql_query(sql="SELECT * FROM ingestion.crm_cust_info", con=conn)

# Clean: remove rows where cst_id is null
df = df.dropna(subset=["cst_id"])

# Clean: remove duplicates on cst_id, keeping the last occurrence
df = df.drop_duplicates(subset=["cst_id"], keep="last")

# Remove leading and trailing spaces from first and last name
df["cst_firstname"] = df["cst_firstname"].str.strip()
df["cst_lastname"] = df["cst_lastname"].str.strip()

# replace marital status codes with full names
df["cst_marital_status"] = df["cst_marital_status"].replace({"M": "Married", "S": "Single"})
df["cst_marital_status"] = df["cst_marital_status"].fillna("N/A")

#replace gender codes with full names
df["cst_gndr"] = df["cst_gndr"].replace({"M": "Male", "F": "Female"})
df["cst_gndr"] = df["cst_gndr"].fillna("N/A")

# Convert cst_id to integer (it's float after dropna)
df["cst_id"] = df["cst_id"].astype(int)

# Drop and recreate the table in transformation schema
cursor.execute("DROP TABLE IF EXISTS transformation.crm_cust_info")
cursor.execute("""
    CREATE TABLE transformation.crm_cust_info (
        cst_id INTEGER,
        cst_key VARCHAR(50),
        cst_firstname VARCHAR(50),
        cst_lastname VARCHAR(50),
        cst_marital_status VARCHAR(50),
        cst_gndr VARCHAR(50),
        cst_create_date DATE
    )
""")

# Insert cleaned data into transformation layer
placeholders = ",".join(["?" for _ in range(len(df.columns))])
for _, row in df.iterrows():
    values = [None if pd.isna(v) else v for v in row]
    cursor.execute(f"INSERT INTO transformation.crm_cust_info VALUES ({placeholders})", values)

print(f"Loaded {len(df)} cleaned rows into transformation.crm_cust_info")

cursor.close()
conn.close()


