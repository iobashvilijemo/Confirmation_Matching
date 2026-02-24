import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st


DB_PATH = Path("DB") / "confirmation.db"
TABLE_NAME = "confirmation_data"
VALIDATION_COLUMNS = [
    "currency_validation",
    "settlement_amount_validation",
    "buy_sell_validation",
    "isin_validation",
    "settlement_date_validation",
    "SSI_validation",
]
DISPLAY_COLUMNS = [
    "id",
    "creation_date",
    "currency",
    "currency_LLM",
    "currency_validation",
    "settlement_amount",
    "settlement_amount_LLM",
    "settlement_amount_validation",
    "buy_sell",
    "buy_sell_LLM",
    "buy_sell_validation",
    "isin",
    "isin_LLM",
    "isin_validation",
    "settlement_date",
    "settlement_date_LLM",
    "settlement_date_validation",
    "SSI",
    "SSI_LLM",
    "SSI_validation",
]


@st.cache_data
def load_data(db_path: Path) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    query = f"SELECT * FROM {TABLE_NAME}"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    working = df.copy()
    working["creation_date"] = pd.to_datetime(working["creation_date"], errors="coerce")
    working["matched_field_count"] = (
        (working[VALIDATION_COLUMNS] == "matched").sum(axis=1).astype(int)
    )
    working["unmatched_field_count"] = (
        (working[VALIDATION_COLUMNS] == "unmatched").sum(axis=1).astype(int)
    )
    working["is_fully_matched"] = working["matched_field_count"] == len(VALIDATION_COLUMNS)
    return working


def apply_date_filter(df: pd.DataFrame) -> pd.DataFrame:
    valid_dates = df["creation_date"].dropna()
    if valid_dates.empty:
        st.warning("No valid creation_date values found. Showing all rows.")
        return df

    min_date = valid_dates.min().date()
    max_date = valid_dates.max().date()
    quick_range = st.sidebar.selectbox(
        "Quick range",
        [
            "Custom",
            "Last Week",
            "Last Month",
            "Last 3 Months",
            "Last 6 Months",
            "Last 1 Year",
        ],
        index=0,
    )

    if quick_range == "Custom":
        selected_dates = st.sidebar.date_input(
            "Creation date range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )
        if isinstance(selected_dates, tuple):
            if len(selected_dates) == 2:
                start_date, end_date = selected_dates
            elif len(selected_dates) == 1:
                start_date = end_date = selected_dates[0]
            else:
                start_date, end_date = min_date, max_date
        else:
            start_date = end_date = selected_dates
    else:
        end_date = max_date
        end_ts = pd.Timestamp(end_date)
        if quick_range == "Last Week":
            start_date = (end_ts - pd.Timedelta(days=7)).date()
        elif quick_range == "Last Month":
            start_date = (end_ts - pd.DateOffset(months=1)).date()
        elif quick_range == "Last 3 Months":
            start_date = (end_ts - pd.DateOffset(months=3)).date()
        elif quick_range == "Last 6 Months":
            start_date = (end_ts - pd.DateOffset(months=6)).date()
        else:
            start_date = (end_ts - pd.DateOffset(years=1)).date()

        if start_date < min_date:
            start_date = min_date

    if start_date > end_date:
        st.error("Start date cannot be after end date.")
        return df.iloc[0:0]

    st.sidebar.caption(f"Active range: {start_date} to {end_date}")
    mask = df["creation_date"].dt.date.between(start_date, end_date)
    return df[mask]


def render_kpis(filtered_df: pd.DataFrame) -> None:
    total_transactions = len(filtered_df)
    full_match_count = int(filtered_df["is_fully_matched"].sum())
    overall_match_rate = (full_match_count / total_transactions * 100) if total_transactions else 0.0
    avg_matched_fields = (
        filtered_df["matched_field_count"].mean() if total_transactions else 0.0
    )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Transactions", f"{total_transactions}")
    c2.metric("Fully Matched", f"{full_match_count}")
    c3.metric("Overall Match Rate", f"{overall_match_rate:.2f}%")
    c4.metric("Avg Matched Fields", f"{avg_matched_fields:.2f} / {len(VALIDATION_COLUMNS)}")


def render_match_analysis(filtered_df: pd.DataFrame) -> None:
    if filtered_df.empty:
        st.info("No records found for the selected date range.")
        return

    st.subheader("Field-Level Match Rate")
    field_rates = (
        (filtered_df[VALIDATION_COLUMNS] == "matched").mean().mul(100).round(2).sort_values(ascending=False)
    )
    field_rates.index = field_rates.index.str.replace("_validation", "", regex=False)
    for field_name, rate in field_rates.items():
        st.write(f"{field_name}: {rate:.2f}%")
        st.progress(min(max(int(round(rate)), 0), 100))

    st.subheader("Transaction Match Distribution")
    distribution = (
        filtered_df["matched_field_count"]
        .value_counts()
        .sort_index()
        .rename_axis("matched_fields")
        .to_frame("transactions")
    )
    distribution_display = distribution.reset_index()
    st.markdown(distribution_display.to_html(index=False), unsafe_allow_html=True)

    st.subheader("Mismatch Hotspots")
    mismatch_counts = (filtered_df[VALIDATION_COLUMNS] == "unmatched").sum().sort_values(ascending=False)
    mismatch_counts.index = mismatch_counts.index.str.replace("_validation", "", regex=False)
    mismatch_display = mismatch_counts.rename("unmatched_count").reset_index()
    mismatch_display.columns = ["field", "unmatched_count"]
    st.markdown(mismatch_display.to_html(index=False), unsafe_allow_html=True)


def render_transaction_details(filtered_df: pd.DataFrame) -> None:
    st.subheader("Transaction Details")
    if filtered_df.empty:
        st.info("No transactions to display.")
        return

    display_df = filtered_df.copy()
    display_df["creation_date"] = display_df["creation_date"].dt.strftime("%Y-%m-%d %H:%M:%S")

    transaction_options = [
        f"ID {row.id} | {row.creation_date}" for row in display_df[["id", "creation_date"]].itertuples(index=False)
    ]
    selected_option = st.selectbox("Select transaction", transaction_options)
    if selected_option is None:
        st.warning("Please select a transaction.")
        return
    selected_id = int(selected_option.split("|")[0].replace("ID", "").strip())

    selected_row = display_df.loc[display_df["id"] == selected_id, DISPLAY_COLUMNS].head(1)
    details_df = pd.DataFrame(
        {
            "field": selected_row.columns,
            "value": [str(v) if pd.notna(v) else "" for v in selected_row.iloc[0].tolist()],
        }
    )
    st.markdown(details_df.to_html(index=False), unsafe_allow_html=True)

    st.subheader("Filtered Transactions")
    filtered_display = display_df[DISPLAY_COLUMNS].copy()
    for col in filtered_display.columns:
        filtered_display[col] = filtered_display[col].map(lambda x: "" if pd.isna(x) else str(x))
    st.markdown(filtered_display.to_html(index=False), unsafe_allow_html=True)

    csv_data = display_df[DISPLAY_COLUMNS].to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Download filtered results (CSV)",
        data=csv_data,
        file_name="transaction_match_results.csv",
        mime="text/csv",
    )


def main() -> None:
    st.set_page_config(page_title="Transaction Match Dashboard", layout="wide")
    # Streamlit 1.19 frontend cannot decode some newer Arrow string types (e.g., LargeUtf8).
    # Force legacy dataframe serialization for compatibility.
    try:
        st.set_option("global.dataFrameSerialization", "legacy")
    except Exception:
        pass
    st.title("Transaction Match Dashboard")
    st.caption("Match analytics for confirmation_data in SQLite")

    if not DB_PATH.exists():
        st.error(f"Database not found: {DB_PATH}")
        return

    raw_df = load_data(DB_PATH)
    if raw_df.empty:
        st.warning("No transaction data found.")
        return

    df = add_derived_columns(raw_df)
    st.sidebar.header("Filters")
    filtered_df = apply_date_filter(df)

    render_kpis(filtered_df)
    render_match_analysis(filtered_df)
    render_transaction_details(filtered_df)


if __name__ == "__main__":
    main()
