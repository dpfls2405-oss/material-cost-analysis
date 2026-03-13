from __future__ import annotations


import streamlit as st

from data_loader import load_standardized_data
from calculators import build_product_base, calculate_monthly_totals
from charts import line_monthly_ratio

st.title("📈 Overview")

data = load_standardized_data()
base = build_product_base(data.get("receipt_performance"), data.get("material_cost"))
monthly = calculate_monthly_totals(base)

if monthly.empty:
    st.warning("표시할 데이터가 없습니다. Upload 페이지에서 데이터 적재 후 다시 확인하세요.")
    st.stop()

month_list = monthly["month"].tolist()
selected_month = st.selectbox("기준월", month_list, index=len(month_list)-1)

current = monthly[monthly["month"] == selected_month].iloc[0]

col1, col2, col3, col4 = st.columns(4)
col1.metric("총매출", f"{current['total_sales']:,.0f}")
col2.metric("총자재비", f"{current['total_material_cost']:,.0f}")
col3.metric("재료비율", f"{current['material_ratio']:.2%}" if current["material_ratio"] == current["material_ratio"] else "-")
delta = current["material_ratio_change"]
col4.metric("전월 대비 변화", f"{delta:.2%}" if delta == delta else "-")

st.plotly_chart(line_monthly_ratio(monthly), use_container_width=True)

with st.expander("월별 KPI 테이블", expanded=False):
    display = monthly.copy()
    st.dataframe(display, use_container_width=True)
