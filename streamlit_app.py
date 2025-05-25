# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# 파일 경로
csv_file_path = 'streamlit_data.csv'
metric_file_path = 'metric_summary.csv'

@st.cache_data
def load_data(file_path):
    return pd.read_csv(file_path)

df = load_data(csv_file_path)

# 날짜 처리
if 'date' in df.columns:
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
else:
    st.error("Date column not found in the CSV file.")

# 전처리: 예측 이후 실제값 제거
def preprocess_data(df):
    cutoff_date = pd.to_datetime('2025-04-30')
    cols_to_nan = ['cabbage', 'radish', 'garlic', 'onion', 'daikon', 'cilantro', 'artichoke']
    df.loc[df.index > cutoff_date, cols_to_nan] = np.nan
    return df

df = preprocess_data(df)

# 정확도 테이블 불러오기
metric_summary = pd.read_csv(metric_file_path)
metric_summary.set_index('product', inplace=True)

# 시각화 함수
def plot_predictions_over_time(df, columns, rolling_mean_window):
    fig, ax = plt.subplots(figsize=(14, 7))
    colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k']
    num_colors = len(colors)

    for i, col in enumerate(columns):
        if col in df.columns:
            ax.plot(df.index, df[col], label=col, linewidth=2, color=colors[i % num_colors])
            rolling_mean = df[col].rolling(window=rolling_mean_window).mean()
            ax.plot(df.index, rolling_mean, label=f'{col} ({rolling_mean_window}-day Rolling Mean)', linestyle='--', color=colors[i % num_colors])

    ax.set_xlabel('Date', fontsize=14)
    ax.set_ylabel('Price', fontsize=14)
    ax.legend(fontsize=12)
    ax.grid(True, color='lightgrey', linestyle='--')
    fig.tight_layout()
    st.pyplot(fig)

# 제목
st.title('🥬🧅 농산물 가격 예측 대시보드 📈')
st.markdown("왼쪽에서 품목과 예측모델, 날짜를 입력하면 특정기간 이후 예측 가격이 표시됩니다.")

# 품목 한글 매핑
vegetable_kor_map = {
    'cabbage': '배추',
    'radish': '무',
    'garlic': '마늘',
    'onion': '양파',
    'daikon': '대파',
    'cilantro': '건고추',
    'artichoke': '깻잎'
}

def label_formatter(eng_key):
    return f"{eng_key} ({vegetable_kor_map.get(eng_key, '')})"

# 품목 및 예측 모델 목록
product_columns = list(vegetable_kor_map.keys())
sorted_vegetables = sorted(product_columns)

# 예측 모델 컬럼 필터링 ('_pred_' 포함)
pred_model_columns = sorted([col for col in df.columns if '_pred_' in col])

# 라벨 생성: "품목 (모델명)" 형태로 매핑
label_map = {f"{col.split('_pred_')[0]} ({col.split('_pred_')[1]})": col for col in pred_model_columns}

# 고유 순서 유지 함수
def unique_preserve_order(seq):
    seen = set()
    return [x for x in seq if not (x in seen or seen.add(x))]

# 사이드바 UI
st.sidebar.title('조회 항목 설정')

# 1. 조회 품목 선택
vegetables = st.sidebar.multiselect(
    '조회 품목:', 
    options=sorted_vegetables, 
    format_func=label_formatter
)

# 2. 조회 품목에 맞는 예측 모델 필터링
filtered_label_keys = [
    label for label in label_map.keys()
    if any(veg == label.split(' ')[0] for veg in vegetables)
]

# 3. 세션에서 이전 선택 가져오기
default_selected_labels = st.session_state.get('selected_labels', [])

# 4. 이전 선택값 중 필터에 맞는 것만 남기기
valid_selected_labels = [label for label in default_selected_labels if label in filtered_label_keys]

# 5. 빈 선택 항목 추가
EMPTY_LABEL = '선택 없음'

# 6. available_labels에 빈 항목 포함 및 고유값 유지
available_labels = unique_preserve_order([EMPTY_LABEL] + filtered_label_keys + valid_selected_labels)

# 7. multiselect 호출
selected_labels = st.sidebar.multiselect(
    '예측 모델 선택:',
    options=available_labels,
    default=[EMPTY_LABEL] if not valid_selected_labels else valid_selected_labels,
    key='selected_labels'
)

# 8. 빈 항목 제거 후 실제 선택된 모델 컬럼 목록
selected_models = [label_map[label] for label in selected_labels if label != EMPTY_LABEL and label in label_map]

# 날짜 입력
start_date = st.sidebar.date_input('시작일', df.index.min().date())
end_date = st.sidebar.date_input('마지막일', df.index.max().date())

# 롤링 윈도우 슬라이더
rolling_mean_window = st.sidebar.slider('Rolling Mean Window', min_value=1, max_value=30, value=7)

# 초기 화면 (품목/모델 미선택 시)
if not vegetables and (not selected_models or selected_models == [] or selected_labels == [EMPTY_LABEL]):
    st.info("👈 왼쪽 사이드바에서 품목과 예측 모델을 선택하세요.")
    st.subheader("📋 전체 품목별 모델 정확도 %")

    metric_percent = (metric_summary * 100).round(2)
    st.dataframe(metric_percent, use_container_width=True)

    with st.expander("📋 전체 정확도 테이블 자세히 보기"):
        st.dataframe(metric_summary, use_container_width=True)

    st.markdown("""
    ---
    📌 **데이터 출처:** [농림축산식품부 통계누리](https://data.mafra.go.kr/main.do)  
    🔎 본 대시보드의 예측 결과는 정부 공개 데이터를 기반으로 생성되었습니다.  
    예측 모델은 과거 가격 패턴을 학습하여 향후 농산물 가격 변동을 추정합니다.  
    본 결과는 참고용이며 실제 가격과는 차이가 발생할 수 있습니다.
    """)
else:
    # 필터링된 데이터
    filtered_df = df.loc[start_date:end_date]

    st.subheader('📈 품목별 실제 가격 + 예측 결과')
    plot_columns = vegetables + selected_models
    plot_predictions_over_time(filtered_df, plot_columns, rolling_mean_window)

    with st.expander("📈 예측값 (최신순 정렬)"):
        if selected_models:
            pred_df = filtered_df[selected_models].copy()
            pred_df_sorted = pred_df.sort_index(ascending=False)
            st.dataframe(pred_df_sorted, use_container_width=True)
        else:
            st.info("예측 모델이 선택되지 않았습니다.")

    if selected_models:
        st.subheader('📊 선택한 예측 모델의 정확도 Summary (퍼센트)')

        model_splits = [col.split('_pred_') for col in selected_models]
        selected_products = list(set([split[0] for split in model_splits]))
        selected_model_names = list(set([split[1] for split in model_splits]))

        for product, model in model_splits:
            try:
                value = metric_summary.loc[product, model]
                percent_value = round(value * 100, 2)
                st.metric(label=f"{product} + {model}", value=f"{percent_value}%")
            except KeyError:
                st.warning(f"{product} + {model} 에 대한 정확도 정보가 없습니다.")

        st.success("✔ 정확도는 퍼센트(%)로 변환되어 위에 표시되었습니다.")

        extended_df = metric_summary.loc[metric_summary.index.intersection(selected_products)]

        with st.expander("📋 정확도 테이블 자세히 보기"):
            st.dataframe(extended_df, use_container_width=True)

    with st.expander("🗂 원본 필터링된 데이터프레임 보기"):
        target_columns = vegetables + selected_models
        st.dataframe(filtered_df[target_columns])

    st.markdown("""
    ---
    📌 **데이터 출처:** [농림축산식품부 통계누리](https://data.mafra.go.kr/main.do)  
    🔎 본 대시보드의 예측 결과는 정부 공개 데이터를 기반으로 생성되었습니다.  
    예측 모델은 과거 가격 패턴을 학습하여 향후 농산물 가격 변동을 추정합니다.  
    본 결과는 참고용이며 실제 가격과는 차이가 발생할 수 있습니다.
    """)
