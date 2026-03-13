from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

def line_monthly_ratio(monthly: pd.DataFrame):
    fig = px.line(
        monthly,
        x="month",
        y="material_ratio",
        markers=True,
        title="월별 재료비율 추이",
    )
    fig.update_yaxes(tickformat=".1%")
    return fig

def bar_contribution(df: pd.DataFrame, title: str):
    fig = px.bar(
        df.sort_values("contribution", ascending=True),
        x="contribution",
        y="product_name",
        orientation="h",
        title=title,
        hover_data=["product_id", "sales_amount", "material_cost"],
    )
    fig.update_xaxes(tickformat=".2%")
    return fig

def waterfall_contribution(start_ratio: float, contrib_df: pd.DataFrame, end_ratio: float):
    x = ["전월 재료비율"] + contrib_df["product_name"].tolist() + ["당월 재료비율"]
    y = [start_ratio] + contrib_df["contribution"].tolist() + [end_ratio]
    measure = ["absolute"] + ["relative"] * len(contrib_df) + ["total"]
    fig = go.Figure(go.Waterfall(x=x, y=y, measure=measure))
    fig.update_layout(title="재료비율 Waterfall")
    fig.update_yaxes(tickformat=".2%")
    return fig

def line_product_metrics(product_df: pd.DataFrame):
    melt = product_df.melt(
        id_vars=["month"],
        value_vars=["sales_amount", "material_cost", "receipt_qty"],
        var_name="metric",
        value_name="value",
    )
    return px.line(melt, x="month", y="value", color="metric", markers=True, title="품목 월별 추이")

def bar_material_gap(material_df: pd.DataFrame, title: str):
    chart_df = material_df.copy().sort_values("usage_gap_qty", ascending=False).head(20)
    fig = px.bar(chart_df, x="usage_gap_qty", y="material_name", orientation="h", title=title)
    return fig
