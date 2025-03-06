import psycopg2
import csv
import json

HOST = "XXXXXXXX"
PORT = "XXXXXXX" 
DATABASE = "XXXX"
USER = "XXXXXXXX"
PASSWORD = "XXXXXXXXXX"

json_filename = "./config.json"
try:
    with open(json_filename, "r") as file:
        queries_data = json.load(file)
except Exception as e:
    print(f"❌ Error reading JSON file: {e}")
    exit(1)

try:
    conn = psycopg2.connect(
        dbname=DATABASE,
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT
    )
    print("✅ Connected to Redshift successfully!")

    cursor = conn.cursor()

    for key, value in queries_data.items():
        query = value.get("SQL", "").strip()

        if not query:
            print(f"⚠️ Skipping {key}: No SQL query found.")
            continue

        try:
            cursor.execute(query)
            rows = cursor.fetchall()

            csv_filename = f"{key}.csv"

            with open(csv_filename, mode="w", newline="") as file:
                writer = csv.writer(file)
                writer.writerow([desc[0] for desc in cursor.description]) 
                writer.writerows(rows)

            print(f"✅ Query results for '{key}' saved to {csv_filename}")

        except Exception as e:
            print(f"❌ Error executing query for '{key}': {e}")

except Exception as e:
    print(f"❌ Connection error: {e}")

finally:
    if 'conn' in locals():
        cursor.close()
        conn.close()
        print("🔌 Connection closed.")
