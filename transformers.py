from __future__ import annotations

import pandas as pd
from helpers import to_number, pct_to_float, normalize_text

def standardize_receipt(df: pd.DataFrame, month: str, source_file_name: str) -> pd.DataFrame:
    # product_id: 단품코드+색상 조합 (BOM과 동일한 방식으로 매칭되도록)
    if "색상" in df.columns:
        product_id = (
            normalize_text(df["단품코드"]).fillna("") +
            normalize_text(df["색상"]).fillna("")
        )
    else:
        product_id = normalize_text(df["단품코드"])

    # 빈 product_id 행 제거
    df = df.copy()
    df = df[product_id.str.strip() != ''].reset_index(drop=True)
    if "색상" in df.columns:
        product_id = (
            normalize_text(df["단품코드"]).fillna("") +
            normalize_text(df["색상"]).fillna("")
        )
    else:
        product_id = normalize_text(df["단품코드"])

    out = pd.DataFrame({
        "month": month,
        "product_id": product_id,
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
    # product_id: 단품컬러가 있으면 단품코드+단품컬러 조합, 없으면 단품코드만
    if "단품컬러" in df.columns:
        product_id = (
            normalize_text(df["단품코드"]).fillna("") +
            normalize_text(df["단품컬러"]).fillna("")
        )
    else:
        product_id = normalize_text(df["단품코드"])

    out = pd.DataFrame({
        "month": month,
        "product_id": product_id,
        "material_id": normalize_text(df["자재코드"]),
        "material_name": normalize_text(df["자재명칭"]),
        "material_group": normalize_text(df["자재구분"]) if "자재구분" in df.columns else None,
        "usage_type": normalize_text(df["사용구분"]) if "사용구분" in df.columns else None,
        "unit_cost": to_number(df["자재단가"]) if "자재단가" in df.columns else None,
        "unit_qty": to_number(df["소요량"]),
        "bom_amount": to_number(df["금액"]) if "금액" in df.columns else None,
        "source_file_name": source_file_name,
    })
    out = out.groupby(["month", "product_id", "material_id"], as_index=False).agg({
        "material_name": "first",
        "material_group": "first",
        "usage_type": "first",
        "unit_cost": "max",
        "unit_qty": "sum",
        "bom_amount": "sum",
        "source_file_name": "first",
    })
    return out

def standardize_purchase(df: pd.DataFrame, month: str, source_file_name: str) -> pd.DataFrame:
    # material_id: 자재코드+색상 조합으로 색상별 구분
    if "색상" in df.columns:
        material_id = (
            normalize_text(df["자재코드"]).fillna("") +
            normalize_text(df["색상"]).fillna("")
        )
        color = normalize_text(df["색상"])
    else:
        material_id = normalize_text(df["자재코드"])
        color = None

    out = pd.DataFrame({
        "month": month,
        "material_id": material_id,
        "material_code": normalize_text(df["자재코드"]),
        "material_color": color,
        "material_name": normalize_text(df["자재명"]),
        "vendor_name": normalize_text(df["거래처명"]) if "거래처명" in df.columns else None,
        "purchase_qty": to_number(df["입고량"]),
        "purchase_amount": to_number(df["입고금액"]),
        "account_type": normalize_text(df["계정구분"]) if "계정구분" in df.columns else None,
        "source_file_name": source_file_name,
    })
    out = out.groupby(["month", "material_id", "vendor_name"], as_index=False).agg({
        "material_code": "first",
        "material_color": "first",
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


def standardize_jit_materials(df: pd.DataFrame, month: str, source_file_name: str) -> pd.DataFrame:
    """JIT 자재 목록 표준화 — 자재코드+색상 조합을 material_id로 사용"""
    # 발주이력 원본에서 왔을 경우 필요한 컬럼 매핑
    code_col = "자재코드"
    color_col = "색상" if "색상" in df.columns else None
    name_col = "자재명" if "자재명" in df.columns else "자재명칭"

    if color_col:
        material_id = (
            normalize_text(df[code_col]).fillna("") +
            normalize_text(df[color_col]).fillna("")
        )
        color = normalize_text(df[color_col])
    else:
        material_id = normalize_text(df[code_col])
        color = None

    out = pd.DataFrame({
        "month": month,
        "material_id": material_id,
        "material_code": normalize_text(df[code_col]),
        "material_color": color,
        "material_name": normalize_text(df[name_col]),
        "vendor_name": normalize_text(df["거래처명"]) if "거래처명" in df.columns else None,
        "unit_cost": to_number(df["자재단가"]) if "자재단가" in df.columns else None,
        "unit": normalize_text(df["단위"]) if "단위" in df.columns else None,
        "order_policy": normalize_text(df["발주방침"]) if "발주방침" in df.columns else None,
        "production_mgmt_no": normalize_text(df["생산관리번호"]) if "생산관리번호" in df.columns else None,
        "source_file_name": source_file_name,
    })

    # 빈 material_id 제거 후 중복 제거 (같은 월 내 자재코드+색상 기준)
    out = out[out["material_id"].str.strip() != ""]
    out = out.drop_duplicates(subset=["month", "material_id"], keep="first")
    return out.reset_index(drop=True)


TRANSFORMER_MAP["jit_materials"] = standardize_jit_materials
