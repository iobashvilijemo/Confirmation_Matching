import json
import sqlite3
from pathlib import Path

import ollama

from llm_metadata import FIELD_LLM_METADATA, FieldLLMMetadata

MODEL = "llama3.2:latest"
DB_PATH = Path("DB") / "confirmation.db"
EXTERNAL_DATA_DIR = Path("External_Data")


def _has_value(value) -> bool:
    if value is None:
        return False
    if isinstance(value, str) and not value.strip():
        return False
    return True


def _extract_column_value(raw_value, metadata: FieldLLMMetadata):
    user_prompt = (
        f"{metadata.few_shot}\n\n"
        f"Input:\n{raw_value}\n\n"
        "Return ONLY the JSON object."
    )

    response = ollama.chat(
        model=MODEL,
        messages=[
            {"role": "system", "content": metadata.system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        format=metadata.format_schema,
        options={"temperature": 0.0},
    )
    parsed = json.loads(response["message"]["content"])
    return parsed.get(metadata.output_key)


def _fetch_rows(conn: sqlite3.Connection):
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT
            id,
            currency, currency_LLM,
            settlement_amount, settlement_amount_LLM,
            buy_sell, buy_sell_LLM,
            isin, isin_LLM,
            settlement_date, settlement_date_LLM,
            SSI, SSI_LLM
        FROM confirmation_data
        """
    )
    return cursor.fetchall()


def _load_transaction_text(row_id: int, base_dir: Path = EXTERNAL_DATA_DIR) -> str | None:
    file_path = base_dir / f"TX{row_id:06d}.txt"
    if not file_path.exists():
        return None
    return file_path.read_text(encoding="utf-8")


def _update_llm_column(conn: sqlite3.Connection, row_id: int, llm_column: str, value) -> None:
    cursor = conn.cursor()
    cursor.execute(
        f"UPDATE confirmation_data SET {llm_column} = ? WHERE id = ?",
        (value, row_id),
    )


def process_new_raw_rows(db_path: Path = DB_PATH) -> int:
    conn = sqlite3.connect(db_path)
    updated_values = 0

    try:
        rows = _fetch_rows(conn)
        for row in rows:
            row_id = row["id"]
            transaction_text = _load_transaction_text(row_id)
            if not _has_value(transaction_text):
                print(
                    f"Row {row_id}: skipped (missing or empty "
                    f"External_Data/TX{row_id:06d}.txt)"
                )
                continue

            for metadata in FIELD_LLM_METADATA.values():
                llm_value = row[metadata.llm_column]

                # Process only missing LLM outputs using the transaction text file as input.
                if _has_value(llm_value):
                    continue

                parsed_value = _extract_column_value(transaction_text, metadata)
                _update_llm_column(conn, row_id, metadata.llm_column, parsed_value)
                updated_values += 1

                print(
                    f"Row {row_id}: {metadata.source_column} -> "
                    f"{metadata.llm_column} = {parsed_value}"
                )

        conn.commit()
        return updated_values
    finally:
        conn.close()


if __name__ == "__main__":
    count = process_new_raw_rows()
    print(f"Completed. Updated {count} LLM column value(s).")
