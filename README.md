# Material Ratio Dashboard

Streamlit dashboard for month-over-month material ratio analysis.

## What it does

- 월별 총매출 / 총자재비 / 재료비율 KPI
- 전월 대비 재료비율 변화
- 재료비율 상승 TOP 품목 / 하락 TOP 품목
- 품목별 기여도(Contribution), Mix 영향, Rate 영향
- 월별 BOM / 구매 / 재고 기반 자재 분석
- CSV 업로드 후 Supabase 적재(옵션)

## Assumptions used in this project

- `입고금액` = 매출
- `입고수량` = 수량 기준
- `총자재비` = 품목별 재료비
- BOM은 월별 스냅샷
- 재고는 `inventory_begin` 중심으로 관리
- 실제 사용량은 아래 우선순위로 계산
  1. `inventory_end`가 있으면 `begin + purchase - end`
  2. 없고 다음달 `inventory_begin`이 있으면 `begin + purchase - next_begin`
  3. 둘 다 없으면 사용량 계산 제외

## Expected file names

```text
YYYY-MM_receipt_performance.csv
YYYY-MM_material_cost.csv
YYYY-MM_bom.csv
YYYY-MM_purchase.csv
YYYY-MM_inventory_begin.csv
YYYY-MM_inventory_end.csv   # optional after first month
```

## Local run

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Supabase setup

1. Create tables with `db/schema.sql`
2. Copy `.streamlit/secrets.toml.example` to `.streamlit/secrets.toml`
3. Fill `SUPABASE_URL` and `SUPABASE_KEY`

## Main pages

- Upload: CSV validation + Supabase upsert
- Overview: KPI and monthly trend
- Contribution: TOP products and waterfall
- Product Drilldown: product-level root cause
- Material Analysis: BOM / inventory / purchase view

## Data model summary

### Product-level datasets
- receipt_performance
- material_cost

### Material-level datasets
- bom_monthly
- purchase
- inventory_begin
- inventory_end

## Core formulas

### Overall material ratio
```text
material_ratio = total_material_cost / total_sales
```

### MoM change
```text
delta_ratio = ratio_t - ratio_t_1
```

### Product contribution
```text
contribution_i
= (material_cost_i,t / total_sales_t)
- (material_cost_i,t-1 / total_sales_t-1)
```

### Mix effect
```text
mix_effect_i
= (sales_share_i,t - sales_share_i,t-1) * product_material_ratio_i,t-1
```

### Rate effect
```text
rate_effect_i
= sales_share_i,t * (product_material_ratio_i,t - product_material_ratio_i,t-1)
```

