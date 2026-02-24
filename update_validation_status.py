import sqlite3
from pathlib import Path


DB_PATH = Path("DB") / "confirmation.db"
TABLE_NAME = "confirmation_data"


def _normalize(value):
    """Normalize values for stable comparisons across TEXT/REAL columns."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return str(value).strip()
    return str(value).strip()


def _normalize_buy_sell(value):
    """Normalize buy/sell tokens to canonical values."""
    if value is None:
        return None

    token = str(value).strip().lower()
    if not token:
        return None

    buy_tokens = {"buy", "b", "purchase", "long"}
    sell_tokens = {"sell", "s", "short", "dispose"}

    if token in buy_tokens:
        return "buy"
    if token in sell_tokens:
        return "sell"
    return token


def update_validation_statuses(db_path: Path = DB_PATH) -> None:
    pairs = [
        ("currency", "currency_LLM", "currency_validation"),
        ("settlement_amount", "settlement_amount_LLM", "settlement_amount_validation"),
        ("buy_sell", "buy_sell_LLM", "buy_sell_validation"),
        ("isin", "isin_LLM", "isin_validation"),
        ("settlement_date", "settlement_date_LLM", "settlement_date_validation"),
        ("SSI", "SSI_LLM", "SSI_validation"),
    ]

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Normalize buy_sell_LLM in table before validation comparison.
    cur.execute(f"SELECT id, buy_sell_LLM FROM {TABLE_NAME}")
    buy_sell_rows = cur.fetchall()
    normalized_buy_sell_updates = [
        (_normalize_buy_sell(llm_value), row_id) for row_id, llm_value in buy_sell_rows
    ]
    cur.executemany(
        f"UPDATE {TABLE_NAME} SET buy_sell_LLM = ? WHERE id = ?",
        normalized_buy_sell_updates,
    )

    for source_col, llm_col, validation_col in pairs:
        cur.execute(f"SELECT id, {source_col}, {llm_col} FROM {TABLE_NAME}")
        rows = cur.fetchall()

        updates = []
        for row_id, source_value, llm_value in rows:
            if source_col == "buy_sell":
                left = _normalize_buy_sell(source_value)
                right = _normalize_buy_sell(llm_value)
            else:
                left = _normalize(source_value)
                right = _normalize(llm_value)
            status = "matched" if (left is not None and right is not None and left == right) else "unmatched"
            updates.append((status, row_id))

        cur.executemany(
            f"UPDATE {TABLE_NAME} SET {validation_col} = ? WHERE id = ?",
            updates,
        )

    conn.commit()
    conn.close()
    print(f"Validation columns updated in {db_path}")


if __name__ == "__main__":
    update_validation_statuses()
