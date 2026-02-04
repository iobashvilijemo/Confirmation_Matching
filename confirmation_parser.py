from pathlib import Path
import json
import ollama

MODEL = "llama3.2:latest"  # Model name to register with ollama
CONFIRMATION_PATH = r"/workspaces/confirmationg-matching-testing/External_Data/1.txt"  # change as needed

FEW_SHOT = """
Example:
"ISIN: US9127123213"
Output:
{"currency":null,"amount":null,"nominal_amount_or_quantity":null,"direction":"unknown","label":null,"isin":"US9127123213","value_or_settlement_date":null,"standard_settlement_instruction":null}

Example:
"Net Consideration : USD   29,851,455.46"
Output:
{"currency":"USD","amount":29851455.46,"nominal_amount_or_quantity":null,"direction":"unknown","label":"Net Consideration","isin":null,"value_or_settlement_date":null,"standard_settlement_instruction":null}

Example:
"QUANTITY: 20,000,000.00"
Output:
{"currency":null,"amount":null,"nominal_amount_or_quantity":20000000.0,"direction":"unknown","label":"Quantity","isin":null,"value_or_settlement_date":null,"standard_settlement_instruction":null}

Example:
"PRIN.AMT: USD 19,750,430.56"
Output:
{"currency":"USD","amount":19750430.56,"nominal_amount_or_quantity":null,"direction":"unknown","label":"Principal Amount","isin":null,"value_or_settlement_date":null,"standard_settlement_instruction":null}

Example:
"SETT DATE: October 21, 2025"
Output:
{"currency":null,"amount":null,"nominal_amount_or_quantity":null,"direction":"unknown","label":"Settlement Date","isin":null,"value_or_settlement_date":"2025-10-21","standard_settlement_instruction":null}

Example:
"Value Date               : 01-Oct-25"
Output:
{"currency":null,"amount":null,"nominal_amount_or_quantity":null,"direction":"unknown","label":"Value Date","isin":null,"value_or_settlement_date":"2025-10-01","standard_settlement_instruction":null}

Example:
"Our SSIs: PSET FFFF33"
Output:
{"currency":null,"amount":null,"nominal_amount_or_quantity":null,"direction":"unknown","label":"Standard Settlement Instruction","isin":null,"value_or_settlement_date":null,"standard_settlement_instruction":"PSET FFFF33"}

Example:
"Our Settlement Instructions
        BANK OF NEW YORK, NEW YORK (BDS)
        FXF"
Output:
{"currency":null,"amount":null,"nominal_amount_or_quantity":null,"direction":"unknown","label":"Standard Settlement Instruction","isin":null,"value_or_settlement_date":null,"standard_settlement_instruction":"BANK OF NEW YORK, NEW YORK (BDS) | FXF"}

Example:
"Accrued Interest              : USD       41,767.96"
Output:
{"currency":"USD","amount":41767.96,"nominal_amount_or_quantity":null,"direction":"unknown","label":"Accrued Interest","isin":null,"value_or_settlement_date":null,"standard_settlement_instruction":null}
""".strip()

SYSTEM_PROMPT = """
You are a deterministic information extraction engine for financial trade confirmations.

Objective:
Extract structured trade information from unstructured trade confirmation text.

You must identify and extract the following properties when present:
- currency
- amount
- nominal_amount_or_quantity
- direction
- isin
- value_or_settlement_date
- standard_settlement_instruction

General rules:
- Output MUST be valid JSON only.
- Do NOT include markdown, commentary, or explanations.
- Always return all fields defined in the JSON schema.
- If a field cannot be identified with high confidence, return null for that field.
- Never infer or guess missing information.

--------------------------------------------------
Field-specific extraction rules:

1. amount (final net cash amount)
- Extract the FINAL net cash amount to be settled.
- Prefer labels such as:
  "Net Amount", "Net Consideration", "Settlement Amount", "Sett Amt".
- If multiple amounts appear:
  1) Prefer settlement amount over net amount if both exist.
  2) Ignore gross amounts, principal, clean price, accrued interest alone.
- Normalize:
  - Parentheses indicate negative values.
  - A leading minus sign indicates negative values.

2. currency
- Extract the ISO 3-letter currency code associated with the extracted amount.
- If the currency cannot be reliably linked to the amount, return null.

3. direction
- Determine cash flow direction only if explicitly stated:
  - "payable by you", "you bought", "we sold to you" → payable_by_us
  - "payable to you", "you sold", "we bought from you" → payable_to_us
- If direction is not explicitly stated or ambiguous, return "unknown".

4. nominal_amount_or_quantity
- Extract the trade size or nominal value when explicitly labeled, such as:
  "Quantity", "Nominal Amount", "Face Amount", "Principal Amount".
- Return a numeric value without separators.
- Do NOT derive quantity from price or consideration.
- If multiple quantities exist, prefer the main security quantity.

5. isin
- Extract the ISIN when explicitly labeled as "ISIN".
- ISIN must be a 12-character alphanumeric code.
- Do NOT infer ISIN from CUSIP or security name.

6. value_or_settlement_date
- Extract the Value Date or Settlement Date when explicitly present.
- Prefer Settlement Date over Value Date if both exist.
- Normalize dates to ISO format: YYYY-MM-DD.
- Do NOT infer dates from trade date or context.

7. standard_settlement_instruction
- Extract settlement instructions when explicitly provided, such as:
  "Our SSIs", "Settlement Instructions", "Delivery Versus Payment".
- Preserve meaningful instruction identifiers (e.g., PSET, BIC, account).
- Condense multi-line instructions into a single readable string.
- Do NOT fabricate or complete missing instructions.

--------------------------------------------------
Failure handling:
- If no final net cash amount is present, return null for amount, currency, and label.
- If the document does not contain trade economics, return null for all fields.

JSON schema:
{
  "currency": string | null,
  "amount": number | null,
  "nominal_amount_or_quantity": number | null,
  "direction": "payable_by_us" | "payable_to_us" | "unknown" | null,
  "isin": string | null,
  "value_or_settlement_date": string | null,
  "standard_settlement_instruction": string | null
}
""".strip()

# Schema-constrained output (best way to force valid JSON)
FORMAT_SCHEMA = {
    "type": "object",
    "properties": {
        "currency": {
            "type": ["string", "null"],
            "description": "ISO 3-letter currency code, e.g. USD"
        },
        "settlement_amount": {
            "type": ["number", "null"],
            "description": "Final net cash amount to be settled"
        },
        "nominal_amount_or_quantity": {
            "type": ["number", "null"],
            "description": "Nominal/principal amount or trade quantity"
        },
        "direction": {
            "type": ["string", "null"],
            "enum": ["payable_by_us", "payable_to_us", "unknown", None],
            "description": "Cash flow direction"
        },

        "isin": {
            "type": ["string", "null"],
            "description": "ISIN code (12-character alphanumeric) if present"
        },
        "value_or_settlement_date": {
            "type": ["string", "null"],
            "description": "Value date or settlement date as found in the confirmation"
        },
        "standard_settlement_instruction": {
            "type": ["string", "null"],
            "description": "SSI / settlement instruction text (condensed)"
        }
    },
    "required": [
        "currency",
        "settlement_amount",
        "nominal_amount_or_quantity",
        "direction",
        "isin",
        "value_or_settlement_date",
        "standard_settlement_instruction"
    ],
    "additionalProperties": False
}


def extract_settlement_json(confirmation_text: str) -> dict:
    user_prompt = f"""{FEW_SHOT}

CONFIRMATION TEXT:
\"\"\"{confirmation_text}\"\"\"

Return ONLY the JSON object.
"""

    resp = ollama.chat(
        model=MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        format=FORMAT_SCHEMA,
        options={"temperature": 0.0},
    )

    # content should already be valid JSON per schema, but we parse to guarantee a dict
    return json.loads(resp["message"]["content"])

if __name__ == "__main__":
    # Create result directory if it doesn't exist
    result_dir = Path(r"/workspaces/confirmationg-matching-testing/result")
    result_dir.mkdir(exist_ok=True)
    
    for i in range(1,3):
        CONFIRMATION_PATH = rf"/workspaces/confirmationg-matching-testing/External_Data/dummy/dummy_{i}.txt"
        confirmation_text = Path(CONFIRMATION_PATH).read_text(encoding="utf-8", errors="replace")
        result = extract_settlement_json(confirmation_text)
        
        # Extract filename from input path and create output path with .json extension
        input_filename = Path(CONFIRMATION_PATH).stem
        output_path = result_dir / f"{input_filename}.json"
        
        # Write result to file
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        print(f"Processed: {input_filename} -> {output_path}")
        print(json.dumps(result, ensure_ascii=False))