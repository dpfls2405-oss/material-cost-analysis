from __future__ import annotations

import pandas as pd
from helpers import parse_filename

REQUIRED_COLUMNS = {
    "receipt_performance": ["단품코드", "단품명", "입고수량", "입고금액"],
    "material_cost": ["코드", "단품명칭", "총자재비"],
    "bom": ["단품코드", "자재코드", "자재명칭", "소요량"],
    "purchase": ["자재코드", "자재명", "입고량", "입고금액"],
    "inventory_begin": ["자재코드", "자재명", "현재고", "현재고금액"],
    "inventory_end": ["자재코드", "자재명", "현재고", "현재고금액"],
}


def validate_filename(file_name: str) -> tuple[str, str]:
    return parse_filename(file_name)


def validate_required_columns(df: pd.DataFrame, dataset_type: str) -> list[str]:
    missing = [col for col in REQUIRED_COLUMNS[dataset_type] if col not in df.columns]
    return missing


def drop_empty_keys(df: pd.DataFrame, dataset_type: str) -> tuple[pd.DataFrame, list[str]]:
    key_map = {
        "receipt_performance": ["단품코드"],
        "material_cost": ["코드"],
        "bom": ["단품코드", "자재코드"],
        "purchase": ["자재코드"],
        "inventory_begin": ["자재코드"],
        "inventory_end": ["자재코드"],
    }

    key_cols = key_map[dataset_type]
    working_df = df.copy()

    # 빈 문자열을 NaN처럼 취급
    for col in key_cols:
        if col in working_df.columns:
            working_df[col] = working_df[col].replace(r"^\s*$", pd.NA, regex=True)

    original_rows = len(working_df)
    cleaned_df = working_df.dropna(subset=key_cols).copy()
    removed_rows = original_rows - len(cleaned_df)

    messages = []
    if removed_rows > 0:
        messages.append(f"필수 키 컬럼 빈 값 {removed_rows}행 자동 제거")

    return cleaned_df, messages


def summarize_validation(df: pd.DataFrame, dataset_type: str) -> dict:
    missing = validate_required_columns(df, dataset_type)

    cleaned_df = df.copy()
    key_issues: list[str] = []

    if not missing:
        cleaned_df, key_issues = drop_empty_keys(df, dataset_type)

    return {
        "row_count": len(cleaned_df),
        "missing_columns": missing,
        "key_issues": key_issues,
        "ok": len(missing) == 0,
        "cleaned_df": cleaned_df,
    }
