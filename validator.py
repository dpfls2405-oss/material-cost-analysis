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
    "jit_materials": ["자재코드", "색상", "자재명"],
}

def validate_filename(file_name: str) -> tuple[str, str]:
    return parse_filename(file_name)

def validate_required_columns(df: pd.DataFrame, dataset_type: str) -> list[str]:
    missing = [col for col in REQUIRED_COLUMNS[dataset_type] if col not in df.columns]
    return missing

def drop_empty_key_rows(df: pd.DataFrame, dataset_type: str) -> tuple[pd.DataFrame, int]:
    """키 컬럼이 비어있는 행(합계행 등) 자동 제거 후 (정제된 df, 제거된 행 수) 반환"""
    key_map = {
        "receipt_performance": ["단품코드"],
        "material_cost": ["코드"],
        "bom": ["단품코드", "자재코드"],
        "purchase": ["자재코드"],
        "inventory_begin": ["자재코드"],
        "inventory_end": ["자재코드"],
        "jit_materials": ["자재코드"],
    }
    keys = key_map.get(dataset_type, [])
    before = len(df)
    for col in keys:
        if col in df.columns:
            df = df[df[col].notna() & (df[col].astype(str).str.strip() != "")]
    dropped = before - len(df)
    return df.reset_index(drop=True), dropped

def validate_no_empty_keys(df: pd.DataFrame, dataset_type: str) -> list[str]:
    # drop_empty_key_rows로 사전 정제 후 호출되므로 실질적으로 빈값 없어야 함
    key_map = {
        "receipt_performance": ["단품코드"],
        "material_cost": ["코드"],
        "bom": ["단품코드", "자재코드"],
        "purchase": ["자재코드"],
        "inventory_begin": ["자재코드"],
        "inventory_end": ["자재코드"],
        "jit_materials": ["자재코드"],
    }
    problems = []
    for col in key_map[dataset_type]:
        if df[col].isna().any():
            problems.append(f"필수 키 컬럼 '{col}'에 빈 값이 있습니다.")
    return problems

def summarize_validation(df: pd.DataFrame, dataset_type: str) -> dict:
    missing = validate_required_columns(df, dataset_type)
    # 합계행 등 키 빈값 행 자동 제거
    df, dropped_count = drop_empty_key_rows(df, dataset_type) if not missing else (df, 0)
    key_issues = validate_no_empty_keys(df, dataset_type) if not missing else []
    return {
        "row_count": len(df),
        "dropped_count": dropped_count,   # 제거된 합계행 수 (UI 안내용)
        "cleaned_df": df,                 # 정제된 df (Upload 페이지에서 재사용)
        "missing_columns": missing,
        "key_issues": key_issues,
        "ok": len(missing) == 0 and len(key_issues) == 0,
    }
