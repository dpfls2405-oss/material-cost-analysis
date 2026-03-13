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

def validate_no_empty_keys(df: pd.DataFrame, dataset_type: str) -> list[str]:
    key_map = {
        "receipt_performance": ["단품코드"],
        "material_cost": ["코드"],
        "bom": ["단품코드", "자재코드"],
        "purchase": ["자재코드"],
        "inventory_begin": ["자재코드"],
        "inventory_end": ["자재코드"],
    }
    problems = []
    for col in key_map[dataset_type]:
        if df[col].isna().any():
            problems.append(f"필수 키 컬럼 '{col}'에 빈 값이 있습니다.")
    return problems

def summarize_validation(df: pd.DataFrame, dataset_type: str) -> dict:
    missing = validate_required_columns(df, dataset_type)
    key_issues = validate_no_empty_keys(df, dataset_type) if not missing else []
    return {
        "row_count": len(df),
        "missing_columns": missing,
        "key_issues": key_issues,
        "ok": len(missing) == 0 and len(key_issues) == 0,
    }
