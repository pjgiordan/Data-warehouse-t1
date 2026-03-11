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
df = pd.read_sql_query(sql="SELECT * FROM ingestion.crm_prd_info", con=conn)

# Replace prd_cost < 0 or null with 0
df["prd_cost"] = df["prd_cost"].apply(lambda x: 0 if pd.isna(x) or x < 0 else x)

# Replace prd_line codes with full names
df["prd_line"] = df["prd_line"].str.strip().replace({
    "R": "Road",
    "S": "Sport",
    "M": "Mountain",
    "T": "Touring"
})
df["prd_line"] = df["prd_line"].fillna("Other")

# Extract prd_subcategory from first 5 chars of prd_key (e.g. "CO-RF-" -> "CO_RF")
# and strip those 6 characters from prd_key
df["prd_subcategory"] = df["prd_key"].str[:5].str.replace("-", "_", regex=False)
df["prd_key"] = df["prd_key"].str[6:]

# Fix prd_end_dt: for each product key, set end date = next row's start date - 1 day
df = df.sort_values(["prd_key", "prd_start_dt"]).reset_index(drop=True)
df["prd_end_dt"] = (
    df.groupby("prd_key")["prd_start_dt"]
    .shift(-1) - pd.Timedelta(days=1)
)

# Sort back by prd_id and reorder columns to match the CREATE TABLE definition
df = df.sort_values("prd_id").reset_index(drop=True)
df = df[["prd_id", "prd_key", "prd_subcategory", "prd_nm", "prd_cost", "prd_line", "prd_start_dt", "prd_end_dt"]]

# Drop and recreate the table in transformation schema
cursor.execute("DROP TABLE IF EXISTS transformation.crm_prd_info")
cursor.execute("""
    CREATE TABLE transformation.crm_prd_info (
        prd_id INTEGER,
        prd_key VARCHAR(50),
        prd_subcategory VARCHAR(50),
        prd_nm VARCHAR(50),
        prd_cost INTEGER,
        prd_line VARCHAR(50),
        prd_start_dt DATE,
        prd_end_dt DATE
    )
""")

# Insert cleaned data into transformation layer
placeholders = ",".join(["?" for _ in range(len(df.columns))])
for _, row in df.iterrows():
    values = [None if pd.isna(v) else v for v in row]
    cursor.execute(f"INSERT INTO transformation.crm_prd_info VALUES ({placeholders})", values)

print(f"Loaded {len(df)} cleaned rows into transformation.crm_prd_info")

cursor.close()
conn.close()
