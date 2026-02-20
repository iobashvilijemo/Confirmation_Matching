import sqlite3
from pathlib import Path

import pandas as pd

DEFAULT_WSS_FILE = Path("utility") / "WSS_Data.xlsx"
DEFAULT_DB_PATH = Path("DB") / "confirmation.db"
TARGET_TABLE = "confirmation_data"
VALID_COLUMNS = [
    "creation_date",
    "currency",
    "settlement_amount",
    "buy_sell",
    "isin",
    "settlement_date",
    "SSI",
]
COLUMN_ALIASES = {
    "create_date": "creation_date",
}


def _normalize_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in ("creation_date", "settlement_date"):
        if col not in df.columns:
            continue
        parsed = pd.to_datetime(df[col], errors="coerce")
        df[col] = parsed.dt.strftime("%Y-%m-%d")
        df[col] = df[col].where(parsed.notna(), None) # type: ignore
    return df


def load_wss_data_to_db(
    wss_file: Path = DEFAULT_WSS_FILE,
    db_path: Path = DEFAULT_DB_PATH,
) -> int:
    """Load WSS Excel rows into confirmation_data using matching table columns only."""
    if not wss_file.exists():
        raise FileNotFoundError(f"Excel file not found: {wss_file}")

    if not db_path.exists():
        raise FileNotFoundError(f"Database file not found: {db_path}")

    print(f"Reading {wss_file}...")
    # Excel row 1 is the header; start data import from row 7.
    df = pd.read_excel(wss_file, skiprows=range(1, 6))
    df.columns = [str(col).strip() for col in df.columns]
    df = df.rename(columns=COLUMN_ALIASES)
    df = _normalize_date_columns(df)

    matched_columns = [col for col in VALID_COLUMNS if col in df.columns]
    if not matched_columns:
        raise ValueError(
            "No matching columns found for confirmation_data. "
            f"Excel columns: {list(df.columns)}"
        )

    extra_columns = [col for col in df.columns if col not in VALID_COLUMNS]
    if extra_columns:
        print(f"Ignoring extra columns: {extra_columns}")

    filtered_df = df[matched_columns]
    print(f"Columns used: {matched_columns}")
    print(f"Rows to insert: {len(filtered_df)}")

    with sqlite3.connect(db_path) as conn:
        filtered_df.to_sql(TARGET_TABLE, conn, if_exists="append", index=False)

    print(f"Inserted {len(filtered_df)} row(s) into {TARGET_TABLE}.")
    return len(filtered_df)


if __name__ == "__main__":
    load_wss_data_to_db()
