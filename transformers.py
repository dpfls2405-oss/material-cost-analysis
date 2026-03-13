from __future__ import annotations

import pandas as pd
from helpers import to_number, pct_to_float, normalize_text

def standardize_receipt(df: pd.DataFrame, month: str, source_file_name: str) -> pd.DataFrame:
    out = pd.DataFrame({
        "month": month,
        "product_id": normalize_text(df["단품코드"]),
        "product_name": normalize_text(df["단품명"]),
        "receipt_qty": to_number(df["입고수량"]),
        "sales_amount": to_number(df["입고금액"]),
        "issue_qty": to_number(df["출고수량"]) if "출고수량" in df.columns else None,
        "issue_amount": to_number(df["출고금액"]) if "출고금액" in df.columns else None,
        "stock_qty": to_number(df["재고수량"]) if "재고수량" in df.columns else None,
        "brand": normalize_text(df["브랜드"]) if "브랜드" in df.columns else None,
        "product_category": normalize_text(df["제품구분"]) if "제품구분" in df.columns else None,
        "source_file_name": source_file_name,
    })
    out = out.groupby(["month", "product_id"], as_index=False).agg({
        "product_name": "first",
        "receipt_qty": "sum",
        "sales_amount": "sum",
        "issue_qty": "sum",
        "issue_amount": "sum",
        "stock_qty": "sum",
        "brand": "first",
        "product_category": "first",
        "source_file_name": "first",
    })
    return out

def standardize_material_cost(df: pd.DataFrame, month: str, source_file_name: str) -> pd.DataFrame:
    ratio_col = "제조원가율" if "제조원가율" in df.columns else None
    out = pd.DataFrame({
        "month": month,
        "product_id": normalize_text(df["코드"]),
        "product_name": normalize_text(df["단품명칭"]),
        "material_cost": to_number(df["총자재비"]),
        "manufacturing_cost": to_number(df["제조원가"]) if "제조원가" in df.columns else None,
        "manufacturing_ratio": pct_to_float(df[ratio_col]) if ratio_col else None,
        "series_name": normalize_text(df["시리즈"]) if "시리즈" in df.columns else None,
        "source_file_name": source_file_name,
    })
    out = out.groupby(["month", "product_id"], as_index=False).agg({
        "product_name": "first",
        "material_cost": "sum",
        "manufacturing_cost": "sum",
        "manufacturing_ratio": "max",
        "series_name": "first",
        "source_file_name": "first",
    })
    return out

def standardize_bom(df: pd.DataFrame, month: str, source_file_name: str) -> pd.DataFrame:
    out = pd.DataFrame({
        "month": month,
        "product_id": normalize_text(df["단품코드"]),
        "material_id": normalize_text(df["자재코드"]),
        "material_name": normalize_text(df["자재명칭"]),
        "material_group": normalize_text(df["자재구분"]) if "자재구분" in df.columns else None,
        "unit_cost": to_number(df["자재단가"]) if "자재단가" in df.columns else None,
        "unit_qty": to_number(df["소요량"]),
        "bom_amount": to_number(df["금액"]) if "금액" in df.columns else None,
        "source_file_name": source_file_name,
    })
    out = out.groupby(["month", "product_id", "material_id"], as_index=False).agg({
        "material_name": "first",
        "material_group": "first",
        "unit_cost": "max",
        "unit_qty": "sum",
        "bom_amount": "sum",
        "source_file_name": "first",
    })
    return out

def standardize_purchase(df: pd.DataFrame, month: str, source_file_name: str) -> pd.DataFrame:
    out = pd.DataFrame({
        "month": month,
        "material_id": normalize_text(df["자재코드"]),
        "material_name": normalize_text(df["자재명"]),
        "vendor_name": normalize_text(df["거래처명"]) if "거래처명" in df.columns else None,
        "purchase_qty": to_number(df["입고량"]),
        "purchase_amount": to_number(df["입고금액"]),
        "account_type": normalize_text(df["계정구분"]) if "계정구분" in df.columns else None,
        "source_file_name": source_file_name,
    })
    out = out.groupby(["month", "material_id", "vendor_name"], as_index=False).agg({
        "material_name": "first",
        "purchase_qty": "sum",
        "purchase_amount": "sum",
        "account_type": "first",
        "source_file_name": "first",
    })
    return out

def standardize_inventory_begin(df: pd.DataFrame, month: str, source_file_name: str) -> pd.DataFrame:
    out = pd.DataFrame({
        "month": month,
        "material_id": normalize_text(df["자재코드"]),
        "material_name": normalize_text(df["자재명"]),
        "begin_qty": to_number(df["현재고"]),
        "begin_amount": to_number(df["현재고금액"]),
        "avg_unit_cost": to_number(df["총평균단가"]) if "총평균단가" in df.columns else None,
        "unit_name": normalize_text(df["단위"]) if "단위" in df.columns else None,
        "source_file_name": source_file_name,
    })
    out = out.groupby(["month", "material_id"], as_index=False).agg({
        "material_name": "first",
        "begin_qty": "sum",
        "begin_amount": "sum",
        "avg_unit_cost": "max",
        "unit_name": "first",
        "source_file_name": "first",
    })
    return out

def standardize_inventory_end(df: pd.DataFrame, month: str, source_file_name: str) -> pd.DataFrame:
    out = pd.DataFrame({
        "month": month,
        "material_id": normalize_text(df["자재코드"]),
        "material_name": normalize_text(df["자재명"]),
        "end_qty": to_number(df["현재고"]),
        "end_amount": to_number(df["현재고금액"]),
        "avg_unit_cost": to_number(df["총평균단가"]) if "총평균단가" in df.columns else None,
        "unit_name": normalize_text(df["단위"]) if "단위" in df.columns else None,
        "source_file_name": source_file_name,
    })
    out = out.groupby(["month", "material_id"], as_index=False).agg({
        "material_name": "first",
        "end_qty": "sum",
        "end_amount": "sum",
        "avg_unit_cost": "max",
        "unit_name": "first",
        "source_file_name": "first",
    })
    return out

TRANSFORMER_MAP = {
    "receipt_performance": standardize_receipt,
    "material_cost": standardize_material_cost,
    "bom": standardize_bom,
    "purchase": standardize_purchase,
    "inventory_begin": standardize_inventory_begin,
    "inventory_end": standardize_inventory_end,
}
