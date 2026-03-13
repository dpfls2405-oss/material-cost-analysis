from __future__ import annotations

from pathlib import Path
import pandas as pd
from helpers import parse_filename
from transformers import TRANSFORMER_MAP
from config import supabase_enabled

LOCAL_DATA_DIR = Path("data")


def load_local_raw_files() -> dict[str, pd.DataFrame]:
    if not LOCAL_DATA_DIR.exists():
        return {}
    result: dict[str, list[pd.DataFrame]] = {}
    for path in LOCAL_DATA_DIR.glob("*.csv"):
        try:
            month, dataset_type = parse_filename(path.name)
            raw = pd.read_csv(path, encoding="utf-8-sig")
            std = TRANSFORMER_MAP[dataset_type](raw, month, path.name)
            result.setdefault(dataset_type, []).append(std)
        except Exception:
            continue
    merged = {k: pd.concat(v, ignore_index=True) for k, v in result.items()}
    return merged


def load_standardized_data() -> dict[str, pd.DataFrame]:
    if supabase_enabled():
        # Lazy import so the app starts even if supabase-py has issues at import time
        from supabase_client import fetch_table
        return {
            "receipt_performance": fetch_table("receipt_performance"),
            "material_cost": fetch_table("material_cost"),
            "bom": fetch_table("bom_monthly"),
            "purchase": fetch_table("purchase"),
            "inventory_begin": fetch_table("inventory_begin"),
            "inventory_end": fetch_table("inventory_end"),
        }
    return load_local_raw_files()
