from __future__ import annotations


import streamlit as st

from data_loader import load_standardized_data
from calculators import build_material_analysis
from charts import bar_material_gap

st.title("🧱 Material Analysis")

data = load_standardized_data()
analysis = build_material_analysis(
    data.get("purchase"),
    data.get("inventory_begin"),
    data.get("inventory_end"),
    data.get("bom"),
    data.get("receipt_performance"),
)

if analysis.empty:
    st.warning("자재 분석을 위한 데이터가 충분하지 않습니다.")
    st.stop()

months = sorted(analysis["month"].dropna().unique().tolist())
selected_month = st.selectbox("기준월", months, index=len(months)-1)

month_df = analysis[analysis["month"] == selected_month].copy()

col1, col2, col3 = st.columns(3)
col1.metric("자재 수", f"{month_df['material_id'].nunique():,}")
col2.metric("구매금액", f"{month_df['purchase_amount'].fillna(0).sum():,.0f}")
valid_usage = month_df["actual_usage_qty"].notna().sum()
col3.metric("실사용 계산 가능 자재 수", f"{valid_usage:,}")

st.plotly_chart(bar_material_gap(month_df, "실사용량 - BOM 예상소요량 TOP 20"), use_container_width=True)

show_cols = [
    "material_id", "material_name", "begin_qty", "purchase_qty",
    "calculated_end_qty", "actual_usage_qty", "expected_usage_qty", "usage_gap_qty"
]
existing_cols = [c for c in show_cols if c in month_df.columns]
st.dataframe(month_df[existing_cols].sort_values("usage_gap_qty", ascending=False), use_container_width=True)
