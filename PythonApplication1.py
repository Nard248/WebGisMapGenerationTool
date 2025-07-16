import psycopg2
import sqlite3
import pandas as pd
import os
import sys
import logging

# ✅ PostgreSQL connection info – update these as needed
PG_CONFIG = {
    "host": "wirelesspostgresqlflexible.postgres.database.azure.com",
    "port": 5432,
    "user": "arka",
    "password": "M8r#5bLw$zT2",       
    "dbname": "wiroidb2",                  
}

# ✅ Table name and schema
TABLE_NAME = "public.us_antenna_structure_towers_test25_upd"

# ✅ Output directory
OUTPUT_DIR = r"C:\Users\meloy\Downloads"

# Set up basic logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

def fetch_data_for_state(state_abbr):
    """Query PostgreSQL for tower data for a specific state abbreviation."""
    query = f"""
        SELECT * FROM {TABLE_NAME}
        WHERE struc_state = %s
    """
    with psycopg2.connect(**PG_CONFIG) as conn:
        df = pd.read_sql(query, conn, params=(state_abbr,))
    return df

def save_to_sqlite(df, state_abbr):
    """Save DataFrame to a SQLite file named after the state."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filename = os.path.join(OUTPUT_DIR, f"antenna_data_{state_abbr}.sqlite")
    conn = sqlite3.connect(filename)
    df.to_sql("antenna_towers", conn, index=False, if_exists="replace")
    conn.close()
    logging.info(f"Saved: {filename} ({len(df)} rows)")

def run_pipeline(states):
    """Main function to handle multiple state inputs."""
    for state in states:
        logging.info(f"📡 Processing state: {state}")
        df = fetch_data_for_state(state)
        if not df.empty:
            save_to_sqlite(df, state)
        else:
            logging.warning(f"⚠️ No data found for {state}")

# 🔁 Entry point with optional command-line arguments
if __name__ == "__main__":
    if len(sys.argv) > 1:
        states = sys.argv[1:]
    else:
        states = ["FL", "GA", "CO"]  # fallback default
    run_pipeline(states)

