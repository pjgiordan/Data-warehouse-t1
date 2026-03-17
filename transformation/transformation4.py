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
df = pd.read_sql_query(sql="SELECT * FROM ingestion.erp_loc_a101", con=conn)

# Remove "-" from cid (e.g. AW-00011000 -> AW00011000)
df["cid"] = df["cid"].str.replace("-", "", regex=False)

# Clean cntry: trim, map codes to full names, replace null/empty with NA
df["cntry"] = df["cntry"].str.strip()
df["cntry"] = df["cntry"].replace({
    "USA": "United States",
    "US": "United States",
    "DE": "Germany",
    "": "NA"
})
df["cntry"] = df["cntry"].fillna("NA")

# Drop and recreate the table in transformation schema
cursor.execute("DROP TABLE IF EXISTS transformation.erp_loc_a101")
cursor.execute("""
    CREATE TABLE transformation.erp_loc_a101 (
        cid VARCHAR(50),
        cntry VARCHAR(50)
    )
""")

# Insert cleaned data into transformation layer
placeholders = ",".join(["?" for _ in range(len(df.columns))])
for _, row in df.iterrows():
    values = [None if pd.isna(v) else v for v in row]
    cursor.execute(f"INSERT INTO transformation.erp_loc_a101 VALUES ({placeholders})", values)

print(f"Loaded {len(df)} cleaned rows into transformation.erp_loc_a101")

cursor.close()
conn.close()
