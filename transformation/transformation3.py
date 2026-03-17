import pyodbc
import pandas as pd
from datetime import date

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
df = pd.read_sql_query(sql="SELECT * FROM ingestion.erp_cust_az12", con=conn)

# Remove leading "NAS" prefix from cid (e.g. NASAW00011000 -> AW00011000)
df["cid"] = df["cid"].str.replace(r"^NAS", "", regex=True)

# Replace bdate with null for dates above the current date
today = pd.Timestamp(date.today())
df["bdate"] = pd.to_datetime(df["bdate"], errors="coerce")
df.loc[df["bdate"] > today, "bdate"] = None

# Clean gen: trim, map M/F to full names, replace null/empty with NA
df["gen"] = df["gen"].str.strip()
df["gen"] = df["gen"].replace({"M": "Male", "F": "Female", "": "NA"})
df["gen"] = df["gen"].fillna("NA")

# Drop and recreate the table in transformation schema
cursor.execute("DROP TABLE IF EXISTS transformation.erp_cust_az12")
cursor.execute("""
    CREATE TABLE transformation.erp_cust_az12 (
        cid VARCHAR(50),
        bdate DATE,
        gen VARCHAR(50)
    )
""")

# Insert cleaned data into transformation layer
placeholders = ",".join(["?" for _ in range(len(df.columns))])
for _, row in df.iterrows():
    values = [None if pd.isna(v) else v for v in row]
    cursor.execute(f"INSERT INTO transformation.erp_cust_az12 VALUES ({placeholders})", values)

print(f"Loaded {len(df)} cleaned rows into transformation.erp_cust_az12")

cursor.close()
conn.close()
