"""


Requirements:
    pip install pandas mysql-connector-python

Usage:
    python load_data.py
"""

import pandas as pd
import mysql.connector
from mysql.connector import Error

CSV_PATH   = "US_Accidents_March23.csv"
NROWS      = 1_000_000
HOST       = "127.0.0.1"
PORT       = 3307
USER       = "root"
PASSWORD   = "root"
DATABASE   = "accidents_db"
TABLE      = "accidents"
BATCH_SIZE = 10_000

COLUMNS = [
    "ID", "Severity", "Start_Time", "End_Time",
    "Start_Lat", "Start_Lng",
    "Street", "City", "State",
    "Weather_Condition", "Temperature(F)", "Visibility(mi)",
    "Junction", "Traffic_Signal", "Sunrise_Sunset"
]

RENAME = {
    "Temperature(F)": "Temperature_F",
    "Visibility(mi)": "Visibility_mi"
}

print(f"Reading {NROWS:,} rows from {CSV_PATH} ...")
df = pd.read_csv(CSV_PATH, usecols=COLUMNS, nrows=NROWS, low_memory=False)
df.rename(columns=RENAME, inplace=True)

# Clean booleans → 0/1
for col in ["Junction", "Traffic_Signal"]:
    df[col] = df[col].map({True: 1, False: 0, "True": 1, "False": 0}).fillna(0).astype(int)

# Replace NaN with None (MySQL NULL)
df = df.where(pd.notnull(df), None)

print(f"Rows loaded in memory : {len(df):,}")
print("Connecting to MySQL ...")

try:
    conn = mysql.connector.connect(
        host=HOST, port=PORT, user=USER, password=PASSWORD, database=DATABASE
    )
    cursor = conn.cursor()

    insert_sql = f"""
        INSERT IGNORE INTO {TABLE}
        (ID, Severity, Start_Time, End_Time, Start_Lat, Start_Lng,
         Street, City, State, Weather_Condition, Temperature_F,
         Visibility_mi, Junction, Traffic_Signal, Sunrise_Sunset)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    rows = [tuple(row) for row in df.itertuples(index=False)]
    total = len(rows)

    for i in range(0, total, BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        cursor.executemany(insert_sql, batch)
        conn.commit()
        print(f"  Inserted {min(i + BATCH_SIZE, total):,} / {total:,}", end="\r")

    print(f"\nDone. {total:,} rows inserted into {DATABASE}.{TABLE}")

except Error as e:
    print(f"MySQL error: {e}")

finally:
    if conn.is_connected():
        cursor.close()
        conn.close()
