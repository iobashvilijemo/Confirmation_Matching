import json
import sqlite3
from pathlib import Path

import ollama

from llm_metadata import FIELD_LLM_METADATA, FieldLLMMetadata

MODEL = "llama3.2:latest"
DB_PATH = Path("DB") / "confirmation.db"


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

            for metadata in FIELD_LLM_METADATA.values():
                source_value = row[metadata.source_column]
                llm_value = row[metadata.llm_column]

                # Process only new/unused raw values (source present but LLM output missing).
                if not _has_value(source_value) or _has_value(llm_value):
                    continue

                parsed_value = _extract_column_value(source_value, metadata)
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
