import pandas as pd
import sqlite3
from pathlib import Path

def load_wss_data_to_db():
    """Load WSS_Data.xlsx into SQLite database wss_data table."""
    
    # Define paths
    wss_file = Path(r"C:\Users\user\Desktop\confirmationg matching testing\WSS_Data.xlsx")
    db_path = Path(r"C:\Users\user\Desktop\confirmationg matching testing\DB\confirmation_DB.db")
    
    # Check if files exist
    if not wss_file.exists():
        print(f"Error: {wss_file} not found.")
        return
    
    if not db_path.exists():
        print(f"Error: {db_path} not found.")
        return
    
    try:
        # Read Excel file
        print(f"Reading {wss_file.name}...")
        df = pd.read_excel(wss_file)
        
        print(f"Columns found: {list(df.columns)}")
        print(f"Total rows: {len(df)}")
        
        # Connect to database
        conn = sqlite3.connect(db_path)
        
        # Use pandas to_sql to insert data
        print("\nInserting data into wss_data table...")
        df.to_sql("wss_data", conn, if_exists="append", index=False)
        
        conn.close()
        
        print(f"\n✓ Successfully inserted data into wss_data table in {db_path.name}")
        print(f"✓ Total records inserted: {len(df)}")
    
    except Exception as e:
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    load_wss_data_to_db()
