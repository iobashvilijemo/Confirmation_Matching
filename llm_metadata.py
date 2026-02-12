from dataclasses import dataclass


@dataclass(frozen=True)
class FieldLLMMetadata:
    source_column: str
    llm_column: str
    output_key: str
    few_shot: str
    system_prompt: str
    format_schema: dict


GENERAL_SYSTEM_PROMPT = """
You are a deterministic information extraction engine for financial trade confirmations.

General rules:
- Output MUST be valid JSON only.
- Do NOT include markdown, commentary, or explanations.
- Return only the field requested by the schema for this task.
- If a field cannot be identified with high confidence, return null.
- Never infer or guess missing information.
""".strip()


def _build_system_prompt(field_specific_rules: str) -> str:
    return (
        f"{GENERAL_SYSTEM_PROMPT}\n\n"
        "Field-specific extraction rules:\n"
        f"{field_specific_rules.strip()}"
    )


def _single_field_schema(field_name: str, field_type: str, description: str) -> dict:
    return {
        "type": "object",
        "properties": {
            field_name: {
                "type": [field_type, "null"],
                "description": description,
            }
        },
        "required": [field_name],
        "additionalProperties": False,
    }


FIELD_LLM_METADATA = {
    "currency": FieldLLMMetadata(
        source_column="currency",
        llm_column="currency_LLM",
        output_key="currency",
        few_shot=(
            'Example:\nInput: "USD"\nOutput: {"currency":"USD"}\n\n'
            'Example:\nInput: "U.S. Dollar"\nOutput: {"currency":"USD"}\n\n'
            'Example:\nInput: "unknown"\nOutput: {"currency":null}'
        ),
        system_prompt=_build_system_prompt(
            """
            - Extract the ISO 3-letter currency code.
            - Prefer the currency associated with settlement/net amount when context exists.
            - If currency cannot be reliably linked or determined, return null.
            """
        ),
        format_schema=_single_field_schema(
            field_name="currency",
            field_type="string",
            description="ISO-4217 3-letter currency code",
        ),
    ),
    "settlement_amount": FieldLLMMetadata(
        source_column="settlement_amount",
        llm_column="settlement_amount_LLM",
        output_key="settlement_amount",
        few_shot=(
            'Example:\nInput: "29,851,455.46"\nOutput: {"settlement_amount":29851455.46}\n\n'
            'Example:\nInput: "(1,250.50)"\nOutput: {"settlement_amount":-1250.5}\n\n'
            'Example:\nInput: "N/A"\nOutput: {"settlement_amount":null}'
        ),
        system_prompt=_build_system_prompt(
            """
            - Extract the final net cash amount to be settled when explicit.
            - Prefer labels such as Net Amount, Net Consideration, Settlement Amount, Sett Amt.
            - If multiple amounts appear, prefer settlement/net amount over gross/principal/clean price/accrued interest.
            - Normalize number format:
              - remove separators
              - parentheses indicate negative
              - leading minus sign indicates negative
            - If no reliable settlement amount exists, return null.
            """
        ),
        format_schema=_single_field_schema(
            field_name="settlement_amount",
            field_type="number",
            description="Normalized numeric settlement amount",
        ),
    ),
    "buy_sell": FieldLLMMetadata(
        source_column="buy_sell",
        llm_column="buy_sell_LLM",
        output_key="buy_sell",
        few_shot=(
            'Example:\nInput: "BUY"\nOutput: {"buy_sell":"BUY"}\n\n'
            'Example:\nInput: "we sold to you"\nOutput: {"buy_sell":"SELL"}\n\n'
            'Example:\nInput: "N/A"\nOutput: {"buy_sell":null}'
        ),
        system_prompt=_build_system_prompt(
            """
            - Extract trade side as BUY or SELL only.
            - If side is not explicit, map directional phrases:
              - payable by you / you bought / we sold to you => BUY
              - payable to you / you sold / we bought from you => SELL
            - If ambiguous, return null.
            """
        ),
        format_schema={
            "type": "object",
            "properties": {
                "buy_sell": {
                    "type": ["string", "null"],
                    "enum": ["BUY", "SELL", None],
                    "description": "Trade side",
                }
            },
            "required": ["buy_sell"],
            "additionalProperties": False,
        },
    ),
    "isin": FieldLLMMetadata(
        source_column="isin",
        llm_column="isin_LLM",
        output_key="isin",
        few_shot=(
            'Example:\nInput: "US9127123213"\nOutput: {"isin":"US9127123213"}\n\n'
            'Example:\nInput: "ISIN: XS1111111111"\nOutput: {"isin":"XS1111111111"}\n\n'
            'Example:\nInput: "CUSIP 123456789"\nOutput: {"isin":null}'
        ),
        system_prompt=_build_system_prompt(
            """
            - Extract ISIN only when explicit.
            - ISIN must be exactly 12 alphanumeric characters.
            - Do not infer ISIN from CUSIP, ticker, or security name.
            - If invalid or absent, return null.
            """
        ),
        format_schema=_single_field_schema(
            field_name="isin",
            field_type="string",
            description="12-character ISIN",
        ),
    ),
    "settlement_date": FieldLLMMetadata(
        source_column="settlement_date",
        llm_column="settlement_date_LLM",
        output_key="settlement_date",
        few_shot=(
            'Example:\nInput: "October 21, 2025"\nOutput: {"settlement_date":"2025-10-21"}\n\n'
            'Example:\nInput: "01-Oct-25"\nOutput: {"settlement_date":"2025-10-01"}\n\n'
            'Example:\nInput: "TBD"\nOutput: {"settlement_date":null}'
        ),
        system_prompt=_build_system_prompt(
            """
            - Extract Settlement Date or Value Date when explicit.
            - Prefer Settlement Date over Value Date if both are present.
            - Normalize to ISO format YYYY-MM-DD.
            - Do not infer dates from trade date or context.
            - If date cannot be confidently parsed, return null.
            """
        ),
        format_schema=_single_field_schema(
            field_name="settlement_date",
            field_type="string",
            description="Settlement date normalized to YYYY-MM-DD",
        ),
    ),
    "SSI": FieldLLMMetadata(
        source_column="SSI",
        llm_column="SSI_LLM",
        output_key="SSI",
        few_shot=(
            'Example:\nInput: "PSET FFFF33"\nOutput: {"SSI":"PSET FFFF33"}\n\n'
            'Example:\nInput: "BANK OF NEW YORK, NEW YORK (BDS) | FXF"\n'
            'Output: {"SSI":"BANK OF NEW YORK, NEW YORK (BDS) | FXF"}\n\n'
            'Example:\nInput: ""\nOutput: {"SSI":null}'
        ),
        system_prompt=_build_system_prompt(
            """
            - Extract standard settlement instructions when explicitly provided.
            - Look for SSI-related labels such as Our SSIs, Settlement Instructions, Delivery Versus Payment.
            - Preserve meaningful identifiers (e.g., PSET, BIC, account references).
            - Condense multi-line instructions into a single readable string.
            - Do not fabricate or complete missing details. If absent, return null.
            """
        ),
        format_schema=_single_field_schema(
            field_name="SSI",
            field_type="string",
            description="Standard settlement instruction text",
        ),
    ),
}
