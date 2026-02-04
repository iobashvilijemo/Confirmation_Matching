import sqlite3
import json
from pathlib import Path

def json_to_sqlite():
    """Convert JSON files from result/ directory into SQLite database."""
    
    # Define paths
    result_dir = Path("./DB")
    db_path = result_dir / "confirmation.db"
    
    # Create or connect to SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create table with schema matching the JSON structure
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Counterparty_Data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT NOT NULL,
            currency TEXT,
            settlement_amount REAL,
            nominal_amount_or_quantity REAL,
            direction TEXT,
            label TEXT,
            isin TEXT,
            value_or_settlement_date TEXT,
            standard_settlement_instruction TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Process all JSON files in result directory
    json_files = list(result_dir.glob("*.json"))
    
    if not json_files:
        print("No JSON files found in result directory.")
        conn.close()
        return
    
    for json_file in json_files:
        try:
            # Read JSON file
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            # Insert data into database
            cursor.execute("""
                INSERT INTO Counterparty_Data (
                    filename,
                    currency,
                    settlement_amount,
                    nominal_amount_or_quantity,
                    direction,
                    label,
                    isin,
                    value_or_settlement_date,
                    standard_settlement_instruction
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                json_file.name,
                data.get("currency"),
                data.get("settlement_amount"),
                data.get("nominal_amount_or_quantity"),
                data.get("direction"),
                data.get("label"),
                data.get("isin"),
                data.get("value_or_settlement_date"),
                data.get("standard_settlement_instruction")
            ))
            
            print(f"Inserted: {json_file.name}")
        
        except Exception as e:
            print(f"Error processing {json_file.name}: {e}")
    
    # Commit changes and close connection
    conn.commit()
    
    # Display summary
    cursor.execute("SELECT COUNT(*) FROM Counterparty_Data")
    count = cursor.fetchone()[0]
    
    print(f"\n✓ Database created successfully: {db_path}")
    print(f"✓ Total records inserted: {count}")
    
    conn.close()

if __name__ == "__main__":
    json_to_sqlite()
