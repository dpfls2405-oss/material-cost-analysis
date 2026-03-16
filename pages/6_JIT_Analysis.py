from __future__ import annotations

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from data_loader import load_standardized_data
from calculators import build_material_analysis

st.title("⚡ JIT Analysis")
st.caption("생산관리번호 기준 JIT 자재의 BOM 대비 실사용 차이 분석 — 분실·품질부적합으로 인한 재구매 탐지")

# ── 데이터 로드 ────────────────────────────────────────────
data = load_standardized_data()
jit_master = data.get("jit_materials", pd.DataFrame())

if jit_master.empty:
    st.warning("JIT 자재 목록이 없습니다. Upload 페이지에서 JIT 자재 목록을 업로드해 주세요.")
    st.info("파일명 형식: YYYY-MM_jit_materials.csv")
    st.stop()

analysis = build_material_analysis(
    data.get("purchase"),
    data.get("inventory_begin"),
    data.get("inventory_end"),
    data.get("bom"),
    data.get("receipt_performance"),
)

if analysis.empty:
    st.warning("분석에 필요한 구매/BOM/입고실적 데이터가 없습니다.")
    st.stop()

# ── 월 선택 ───────────────────────────────────────────────
months = sorted(analysis["month"].dropna().unique().tolist())
if not months:
    st.warning("분석 가능한 월 데이터가 없습니다.")
    st.stop()

selected_month = st.selectbox("기준월", months, index=len(months) - 1)

# ── JIT 자재 목록 (선택월 or 가장 최근월) ──────────────────
jit_months = sorted(jit_master["month"].dropna().unique().tolist())
jit_month = selected_month if selected_month in jit_months else (jit_months[-1] if jit_months else None)

if jit_month is None:
    st.warning("JIT 자재 목록 데이터가 없습니다.")
    st.stop()

if jit_month != selected_month:
    st.info(f"선택월({selected_month}) JIT 목록 없음 → 가장 최근 목록({jit_month}) 사용")

jit_ids = set(jit_master[jit_master["month"] == jit_month]["material_id"].dropna().unique())

# ── 분석 데이터 필터링 ─────────────────────────────────────
month_analysis = analysis[analysis["month"] == selected_month].copy()
jit_analysis = month_analysis[month_analysis["material_id"].isin(jit_ids)].copy()
non_jit_count = len(month_analysis) - len(jit_analysis)

# ── KPI ───────────────────────────────────────────────────
st.divider()
col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("JIT 자재 수", f"{len(jit_ids):,}개")
col2.metric("이번 달 JIT 분석 가능", f"{len(jit_analysis):,}개")

gap_qty_count = jit_analysis["usage_gap_qty"].notna().sum()
col3.metric("수량 GAP 계산 가능", f"{gap_qty_count:,}개")

total_gap_amount = jit_analysis["usage_gap_amount"].fillna(0).sum()
col4.metric(
    "구매-예상 금액차이 합계",
    f"{total_gap_amount:,.0f}원",
    delta=f"{total_gap_amount:,.0f}",
    delta_color="inverse",
)

# 초과 구매 자재 수 (양수 gap = 예상보다 더 구매)
over_purchase = (jit_analysis["usage_gap_amount"].fillna(0) > 0).sum()
col5.metric("초과 구매 발생 자재", f"{over_purchase:,}개", delta_color="off")

st.divider()

# ── 탭 구성 ───────────────────────────────────────────────
tab1, tab2, tab3 = st.tabs(["💰 금액 초과 TOP", "📦 수량 GAP TOP", "📋 전체 JIT 자재 현황"])

with tab1:
    st.subheader("구매금액 - BOM 예상소요금액 초과 TOP 30")
    st.caption("양수(+) = 예상보다 더 구매 → 분실·부적합 재구매 의심")

    amt_df = jit_analysis[jit_analysis["usage_gap_amount"].notna()].copy()
    amt_df = amt_df.sort_values("usage_gap_amount", ascending=False)

    if amt_df.empty:
        st.info("금액 GAP 데이터가 없습니다. BOM과 구매 데이터를 확인해 주세요.")
    else:
        top30 = amt_df.head(30).sort_values("usage_gap_amount", ascending=True)
        colors = ["#e74c3c" if v > 0 else "#2ecc71" for v in top30["usage_gap_amount"]]

        fig = go.Figure(go.Bar(
            x=top30["usage_gap_amount"],
            y=top30["material_name"].fillna(top30["material_id"]),
            orientation="h",
            marker_color=colors,
            customdata=top30[["material_id", "purchase_amount", "expected_usage_amount"]].values,
            hovertemplate=(
                "<b>%{y}</b><br>"
                "자재ID: %{customdata[0]}<br>"
                "구매금액: %{customdata[1]:,.0f}원<br>"
                "BOM예상금액: %{customdata[2]:,.0f}원<br>"
                "차이: %{x:,.0f}원<extra></extra>"
            ),
        ))
        fig.update_layout(
            title="JIT 자재 금액 GAP TOP 30 (빨강=초과구매, 초록=절감)",
            xaxis=dict(title="구매금액 - BOM예상금액 (원)", tickformat=","),
            height=700,
        )
        st.plotly_chart(fig, use_container_width=True)

        # 상세 테이블
        show_cols = ["material_id", "material_name", "purchase_qty", "purchase_amount",
                     "expected_usage_qty", "expected_usage_amount", "usage_gap_amount"]
        existing = [c for c in show_cols if c in amt_df.columns]
        _sorted_amt = amt_df.copy()
        if "usage_gap_amount" in _sorted_amt.columns and not _sorted_amt.empty:
            _sorted_amt = _sorted_amt.iloc[_sorted_amt["usage_gap_amount"].abs().argsort()[::-1]]
        st.dataframe(
            _sorted_amt[existing].head(50)
            .rename(columns={
                "material_id": "자재ID", "material_name": "자재명",
                "purchase_qty": "구매수량", "purchase_amount": "구매금액",
                "expected_usage_qty": "BOM예상수량", "expected_usage_amount": "BOM예상금액",
                "usage_gap_amount": "금액차이",
            }),
            use_container_width=True,
        )

with tab2:
    st.subheader("실사용량 - BOM 예상소요량 GAP TOP 30")
    st.caption("양수(+) = 예상보다 더 사용 → 공정 중 분실·불량 의심")

    qty_df = jit_analysis[jit_analysis["usage_gap_qty"].notna()].copy()
    qty_df = qty_df.sort_values("usage_gap_qty", ascending=False)

    if qty_df.empty:
        st.info("수량 GAP 데이터가 없습니다. 재고 데이터를 확인해 주세요.")
    else:
        top30q = qty_df.head(30).sort_values("usage_gap_qty", ascending=True)
        colors_q = ["#e74c3c" if v > 0 else "#2ecc71" for v in top30q["usage_gap_qty"]]

        fig2 = go.Figure(go.Bar(
            x=top30q["usage_gap_qty"],
            y=top30q["material_name"].fillna(top30q["material_id"]),
            orientation="h",
            marker_color=colors_q,
            customdata=top30q[["material_id", "actual_usage_qty", "expected_usage_qty"]].values,
            hovertemplate=(
                "<b>%{y}</b><br>"
                "자재ID: %{customdata[0]}<br>"
                "실사용량: %{customdata[1]:,.1f}<br>"
                "BOM예상량: %{customdata[2]:,.1f}<br>"
                "차이: %{x:,.1f}<extra></extra>"
            ),
        ))
        fig2.update_layout(
            title="JIT 자재 수량 GAP TOP 30 (빨강=초과사용, 초록=절감)",
            xaxis=dict(title="실사용량 - BOM예상량", tickformat=","),
            height=700,
        )
        st.plotly_chart(fig2, use_container_width=True)

        show_cols_q = ["material_id", "material_name", "begin_qty", "purchase_qty",
                       "actual_usage_qty", "expected_usage_qty", "usage_gap_qty"]
        existing_q = [c for c in show_cols_q if c in qty_df.columns]
        _sorted_qty = qty_df.copy()
        if "usage_gap_qty" in _sorted_qty.columns and not _sorted_qty.empty:
            _sorted_qty = _sorted_qty.iloc[_sorted_qty["usage_gap_qty"].abs().argsort()[::-1]]
        st.dataframe(
            _sorted_qty[existing_q].head(50)
            .rename(columns={
                "material_id": "자재ID", "material_name": "자재명",
                "begin_qty": "기초재고", "purchase_qty": "구매수량",
                "actual_usage_qty": "실사용량", "expected_usage_qty": "BOM예상량",
                "usage_gap_qty": "수량차이",
            }),
            use_container_width=True,
        )

with tab3:
    st.subheader("전체 JIT 자재 현황")

    jit_detail = jit_master[jit_master["month"] == jit_month].copy()

    col_a, col_b = st.columns(2)
    with col_a:
        # 거래처별 JIT 자재 수
        if "vendor_name" in jit_detail.columns:
            vendor_count = (
                jit_detail.groupby("vendor_name", dropna=False)["material_id"]
                .count()
                .reset_index()
                .rename(columns={"vendor_name": "거래처명", "material_id": "JIT자재수"})
                .sort_values("JIT자재수", ascending=False)
                .head(15)
            )
            fig3 = px.bar(
                vendor_count.sort_values("JIT자재수"),
                x="JIT자재수", y="거래처명",
                orientation="h", title="거래처별 JIT 자재 수",
                color_discrete_sequence=["#3498db"],
            )
            st.plotly_chart(fig3, use_container_width=True)

    with col_b:
        # 분석 매칭 현황
        matched = len(jit_analysis)
        unmatched = len(jit_ids) - matched
        fig4 = px.pie(
            values=[matched, unmatched],
            names=["분석 매칭됨", "구매/BOM 데이터 없음"],
            title="JIT 자재 분석 매칭 현황",
            color_discrete_sequence=["#2ecc71", "#bdc3c7"],
        )
        st.plotly_chart(fig4, use_container_width=True)

    # JIT 마스터 목록
    st.subheader(f"JIT 자재 목록 ({jit_month} 기준, {len(jit_detail):,}개)")
    display_cols = ["material_id", "material_code", "material_color", "material_name",
                    "vendor_name", "unit_cost", "order_policy", "production_mgmt_no"]
    existing_d = [c for c in display_cols if c in jit_detail.columns]
    st.dataframe(
        jit_detail[existing_d].rename(columns={
            "material_id": "자재ID", "material_code": "자재코드", "material_color": "색상",
            "material_name": "자재명", "vendor_name": "거래처명",
            "unit_cost": "자재단가", "order_policy": "발주방침",
            "production_mgmt_no": "생산관리번호",
        }),
        use_container_width=True,
    )
