from __future__ import annotations

import pandas as pd
import numpy as np


def build_product_base(receipt_df, material_df) -> pd.DataFrame:
    if receipt_df is None:
        receipt_df = pd.DataFrame()
    if material_df is None:
        material_df = pd.DataFrame()
    if receipt_df.empty and material_df.empty:
        return pd.DataFrame()
    base = receipt_df.merge(
        material_df,
        on=["month", "product_id"],
        how="outer",
        suffixes=("", "_mat"),
    )
    if "product_name_mat" in base.columns:
        base["product_name"] = base["product_name"].fillna(base["product_name_mat"])
        base = base.drop(columns=["product_name_mat"])
    base["sales_amount"] = base["sales_amount"].fillna(0.0)
    base["material_cost"] = base["material_cost"].fillna(0.0)
    base["receipt_qty"] = base["receipt_qty"].fillna(0.0)
    base = base.sort_values(["product_id", "month"]).reset_index(drop=True)
    return base


def calculate_monthly_totals(base: pd.DataFrame) -> pd.DataFrame:
    if base is None or base.empty:
        return pd.DataFrame()
    monthly = base.groupby("month", as_index=False).agg(
        total_sales=("sales_amount", "sum"),
        total_material_cost=("material_cost", "sum"),
        total_receipt_qty=("receipt_qty", "sum"),
        product_count=("product_id", "nunique"),
    )
    monthly["material_ratio"] = monthly["total_material_cost"] / monthly["total_sales"].replace(0, np.nan)
    monthly = monthly.sort_values("month").reset_index(drop=True)
    monthly["prev_material_ratio"] = monthly["material_ratio"].shift(1)
    monthly["material_ratio_change"] = monthly["material_ratio"] - monthly["prev_material_ratio"]
    return monthly


def enrich_product_base(base: pd.DataFrame, monthly: pd.DataFrame) -> pd.DataFrame:
    if base is None or base.empty:
        return pd.DataFrame()
    if monthly is None or monthly.empty:
        return pd.DataFrame()
    monthly_map = monthly[["month", "total_sales"]].copy()
    enriched = base.merge(monthly_map, on="month", how="left")
    enriched["product_material_ratio"] = enriched["material_cost"] / enriched["sales_amount"].replace(0, np.nan)
    enriched["sales_share"] = enriched["sales_amount"] / enriched["total_sales"].replace(0, np.nan)

    enriched = enriched.sort_values(["product_id", "month"]).reset_index(drop=True)
    group = enriched.groupby("product_id", dropna=False)
    enriched["prev_sales_amount"] = group["sales_amount"].shift(1)
    enriched["prev_material_cost"] = group["material_cost"].shift(1)
    enriched["prev_receipt_qty"] = group["receipt_qty"].shift(1)
    enriched["prev_product_material_ratio"] = group["product_material_ratio"].shift(1)
    enriched["prev_sales_share"] = group["sales_share"].shift(1)
    enriched["prev_month"] = group["month"].shift(1)

    prev_total_sales_map = monthly.rename(columns={"month": "prev_month", "total_sales": "prev_total_sales"})
    enriched = enriched.merge(prev_total_sales_map[["prev_month", "prev_total_sales"]], on="prev_month", how="left")

    enriched["contribution"] = (
        enriched["material_cost"] / enriched["total_sales"].replace(0, np.nan)
        - enriched["prev_material_cost"] / enriched["prev_total_sales"].replace(0, np.nan)
    )
    enriched["mix_effect"] = (
        (enriched["sales_share"] - enriched["prev_sales_share"]) * enriched["prev_product_material_ratio"]
    )
    enriched["rate_effect"] = (
        enriched["sales_share"] * (enriched["product_material_ratio"] - enriched["prev_product_material_ratio"])
    )
    return enriched


def get_top_contributors(enriched: pd.DataFrame, month: str, top_n: int = 20, ascending: bool = False) -> pd.DataFrame:
    current = enriched[enriched["month"] == month].copy()
    cols = [
        "product_id",
        "product_name",
        "sales_amount",
        "material_cost",
        "receipt_qty",
        "product_material_ratio",
        "prev_product_material_ratio",
        "contribution",
        "mix_effect",
        "rate_effect",
    ]
    current = current[cols].fillna(0)
    return current.sort_values("contribution", ascending=ascending).head(top_n)


def prepare_waterfall_frame(enriched: pd.DataFrame, month: str, top_n: int = 10) -> pd.DataFrame:
    top = get_top_contributors(enriched, month, top_n=top_n, ascending=False).copy()
    return top[["product_name", "contribution"]]


def build_material_usage(purchase_df, inventory_begin_df, inventory_end_df) -> pd.DataFrame:
    if purchase_df is None:
        purchase_df = pd.DataFrame()
    if inventory_begin_df is None:
        inventory_begin_df = pd.DataFrame()
    if inventory_end_df is None:
        inventory_end_df = pd.DataFrame()
    if purchase_df.empty and inventory_begin_df.empty and inventory_end_df.empty:
        return pd.DataFrame()

    begin = inventory_begin_df.copy()
    end = inventory_end_df.copy()
    purchase = purchase_df.groupby(["month", "material_id"], as_index=False).agg(
        material_name=("material_name", "first"),
        purchase_qty=("purchase_qty", "sum"),
        purchase_amount=("purchase_amount", "sum"),
    )

    frame = begin.merge(
        purchase, on=["month", "material_id"], how="outer", suffixes=("_begin", "")
    )
    # Guard: end may be empty (no inventory_end data uploaded yet)
    if not end.empty:
        frame = frame.merge(end[["month", "material_id", "end_qty", "end_amount"]], on=["month", "material_id"], how="left")
    else:
        frame["end_qty"] = np.nan
        frame["end_amount"] = np.nan

    frame["material_name"] = frame["material_name_begin"].fillna(frame["material_name"])
    frame = frame.drop(columns=[c for c in frame.columns if c.endswith("_begin")], errors="ignore")

    # next month's begin as fallback end
    next_begin = begin[["month", "material_id", "begin_qty"]].copy()
    next_begin = next_begin.rename(columns={"month": "next_month", "begin_qty": "next_begin_qty"})
    months = sorted(begin["month"].dropna().unique().tolist())
    month_next_map = {months[i]: months[i + 1] for i in range(len(months) - 1)}
    frame["next_month"] = frame["month"].map(month_next_map)
    frame = frame.merge(next_begin, on=["next_month", "material_id"], how="left")
    frame["calculated_end_qty"] = frame["end_qty"].fillna(frame["next_begin_qty"])
    frame["actual_usage_qty"] = frame["begin_qty"].fillna(0) + frame["purchase_qty"].fillna(0) - frame["calculated_end_qty"]
    frame.loc[frame["calculated_end_qty"].isna(), "actual_usage_qty"] = np.nan
    return frame


def build_bom_expected_usage(bom_df, receipt_df) -> pd.DataFrame:
    if bom_df is None:
        bom_df = pd.DataFrame()
    if receipt_df is None:
        receipt_df = pd.DataFrame()
    if bom_df.empty or receipt_df.empty:
        return pd.DataFrame()
    merged = bom_df.merge(
        receipt_df[["month", "product_id", "receipt_qty"]],
        on=["month", "product_id"],
        how="left",
    )
    merged["receipt_qty"] = merged["receipt_qty"].fillna(0)
    merged["expected_usage_qty"] = merged["unit_qty"].fillna(0) * merged["receipt_qty"]
    merged["expected_usage_amount"] = merged["bom_amount"].fillna(0) * merged["receipt_qty"]
    material_view = merged.groupby(["month", "material_id"], as_index=False).agg(
        material_name=("material_name", "first"),
        expected_usage_qty=("expected_usage_qty", "sum"),
        expected_usage_amount=("expected_usage_amount", "sum"),
    )
    return material_view


def build_material_analysis(purchase_df, inventory_begin_df, inventory_end_df, bom_df, receipt_df) -> pd.DataFrame:
    if purchase_df is None:
        purchase_df = pd.DataFrame()
    if inventory_begin_df is None:
        inventory_begin_df = pd.DataFrame()
    if inventory_end_df is None:
        inventory_end_df = pd.DataFrame()
    if bom_df is None:
        bom_df = pd.DataFrame()
    if receipt_df is None:
        receipt_df = pd.DataFrame()
    usage = build_material_usage(purchase_df, inventory_begin_df, inventory_end_df)
    expected = build_bom_expected_usage(bom_df, receipt_df)
    if usage.empty and expected.empty:
        return pd.DataFrame()
    out = usage.merge(expected, on=["month", "material_id"], how="outer", suffixes=("", "_exp"))
    out["material_name"] = out["material_name"].fillna(out.get("material_name_exp"))
    out["usage_gap_qty"] = out["actual_usage_qty"] - out["expected_usage_qty"]
    return out


def get_product_material_breakdown(bom_df, product_id: str, month: str, receipt_qty: float) -> pd.DataFrame:
    if bom_df is None or (hasattr(bom_df, "empty") and bom_df.empty):
        return pd.DataFrame()
    product_bom = bom_df[(bom_df["product_id"] == product_id) & (bom_df["month"] == month)].copy()
    if product_bom.empty:
        return product_bom
    product_bom["expected_usage_qty"] = product_bom["unit_qty"] * receipt_qty
    product_bom["expected_usage_amount"] = product_bom["bom_amount"].fillna(0) * receipt_qty
    return product_bom.sort_values("expected_usage_amount", ascending=False)
