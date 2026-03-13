import streamlit as st

st.set_page_config(
    page_title="재료비율 분석 대시보드",
    page_icon="📊",
    layout="wide",
)

st.title("재료비율 분석 대시보드")
st.caption("월별 재료비율 변화, 전월 대비 기여도, TOP 품목, 자재 원인 분석")

st.markdown(
    '''
### 시작 순서
1. **Upload** 페이지에서 CSV 형식 검증 및 Supabase 적재
2. **Overview** 에서 월별 KPI 확인
3. **Contribution** 에서 전월 대비 상승 / 하락 TOP 품목 확인
4. **Product Drilldown** 에서 품목별 원인 분석
5. **Material Analysis** 에서 BOM / 구매 / 재고 원인 확인
'''
)

st.info("입고금액=매출, 입고수량=수량 기준, 총자재비=품목 재료비 기준으로 계산합니다.")
