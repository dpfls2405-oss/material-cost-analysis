from __future__ import annotations

import pandas as pd
from config import get_secret, TABLE_MAP


def get_client():
    """Return a Supabase client. Raises ValueError if credentials are missing."""
    try:
        from supabase import create_client
    except ImportError as exc:
        raise ImportError(
            "supabase-py is not installed. Add `supabase>=2.7` to requirements.txt."
        ) from exc

    url = get_secret("SUPABASE_URL")
    key = get_secret("SUPABASE_KEY")
    if not url or not key:
        raise ValueError("Supabase secrets are not configured.")
    return create_client(url, key)

def upsert_dataframe(dataset_type: str, df: pd.DataFrame) -> None:
    client = get_client()
    table_name = TABLE_MAP[dataset_type]
    records = df.where(pd.notna(df), None).to_dict("records")
    if not records:
        return
    client.table(table_name).upsert(records).execute()

def insert_upload_log(month: str, dataset_type: str, source_file_name: str, row_count: int, status: str, message: str = "") -> None:
    client = get_client()
    client.table("upload_log").insert({
        "data_month": month,
        "dataset_type": dataset_type,
        "source_file_name": source_file_name,
        "row_count": row_count,
        "status": status,
        "message": message,
    }).execute()

def fetch_table(table_name: str, columns: str = "*") -> pd.DataFrame:
    client = get_client()
    result = client.table(table_name).select(columns).execute()
    data = result.data or []
    return pd.DataFrame(data)
