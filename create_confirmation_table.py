import sqlite3
from pathlib import Path


def create_confirmation_table(db_path: Path = Path("DB") / "confirmation.db") -> None:
    """Create the confirmation table with source and LLM columns."""
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS confirmation_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            currency TEXT,
            currency_LLM TEXT,
            settlement_amount REAL,
            settlement_amount_LLM TEXT,
            buy_sell TEXT,
            buy_sell_LLM TEXT,
            isin TEXT,
            isin_LLM TEXT,
            settlement_date TEXT,
            settlement_date_LLM TEXT,
            SSI TEXT,
            SSI_LLM TEXT,
            creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            creation_date_LLM TEXT
        )
        """
    )

    conn.commit()
    conn.close()

    print(f"Table 'confirmation_data' is ready in: {db_path}")


if __name__ == "__main__":
    create_confirmation_table()
