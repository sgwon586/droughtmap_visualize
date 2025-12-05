import streamlit as st
import geopandas as gpd
import pandas as pd
import os 
import plotly.express as px 
import plotly.graph_objects as go 
import numpy as np

# -----------------------------------------------------------------------------
# [1] 페이지 기본 설정
# -----------------------------------------------------------------------------
st.set_page_config(page_title="강원도 가뭄 위험도 분석", layout="wide")

st.title("🗺️ 강원특별자치도 가뭄 위험도 & 뉴스 반응(SII) 분석")

# 상단 설명 텍스트
st.markdown("""
<style>
    .info-text { font-size:16px !important; font-family: "Malgun Gothic"; line-height: 1.8; }
</style>
<div class='info-text'>
    <b>📊 분석 방식:</b><br>
    사용자가 설정한 <b>PVI(가뭄 심각도)</b>와 <b>SII(사회적 관심도)</b>의 기준값을 바탕으로 지역을 4가지 유형으로 분류합니다.<br>
</div>
""", unsafe_allow_html=True)

st.subheader("📋 카테고리 상세 정의")
image_path = "category.png"

#상단 설명 이미지 표시
if os.path.exists(image_path):
    # 지도 크기를 1000px로 맞춤
    st.image(image_path, caption="[그림] PVI와 SII 지표에 따른 4분면 분류 기준", width=1000)
else:
    st.warning("⚠️ 'category.png' 파일을 찾을 수 없습니다. 실행 파일과 같은 위치에 이미지를 넣어주세요.")

st.divider()

# -----------------------------------------------------------------------------
# [2] 데이터 로드 및 전처리 함수
# -----------------------------------------------------------------------------
@st.cache_data
def load_and_process_data():
    map_files = ["gangwon_map_simplified.parquet", "processed_gangwon_analaysis.parquet"]
    map_path = None
    for f in map_files:
        if os.path.exists(f):
            map_path = f
            break
    
    if not map_path:
        return None, "지도 파일(parquet)을 찾을 수 없습니다."
    
    # 지도 파일 로드
    gdf = gpd.read_parquet(map_path)
    if gdf.crs != "epsg:4326":
        gdf = gdf.to_crs(epsg=4326)
    
    # CSV 데이터 로드
    data_dir = "DATA"
    pvi_path = os.path.join(data_dir, "pvi_result_final.csv")
    news_path = os.path.join(data_dir, "강원도_지역별_뉴스갯수.csv")
    
    # 없는 경우를 위한 예외처리
    if not os.path.exists(pvi_path) or not os.path.exists(news_path):
        return None, "DATA 폴더 내에 CSV 파일이 없습니다."
    
    #인코딩 처리 -> 어떤 인코딩 방식인지 모를때를 대비하기 위함
    try: df_pvi = pd.read_csv(pvi_path, encoding='utf-8')
    except: df_pvi = pd.read_csv(pvi_path, encoding='cp949')

    try: df_news = pd.read_csv(news_path, encoding='euc-kr')
    except:
        try: df_news = pd.read_csv(news_path, encoding='utf-8')
        except: df_news = pd.read_csv(news_path, encoding='cp949')
    # 데이터 로드 완료. 두 번째 인자는 에러 메세지인데, 없으므로 None 반환
    return (gdf, df_pvi, df_news), None



def normalize_region_name(name):
    # 지역명 정규화 함수
    if pd.isna(name): return ""
    #공백 제거 및 접미사 처리
    name = str(name).strip()
    name = name.replace("강원특별자치도", "").replace("강원도", "").strip()
    if len(name) > 1:
        if name.endswith("시") or name.endswith("군"):
            return name[:-1]
    return name

# SII 점수 계산 함수 (로그 정규화)
def calculate_sii_score(df, col_name):
    df['log_val'] = np.log1p(df[col_name])
    min_val = df['log_val'].min()
    max_val = df['log_val'].max()
    if max_val - min_val == 0:
        df['SII_Score'] = 0.0
    else:
        df['SII_Score'] = (df['log_val'] - min_val) / (max_val - min_val)
    return df

# -----------------------------------------------------------------------------
# [3] 메인 로직 실행
# -----------------------------------------------------------------------------
data_tuple, error_msg = load_and_process_data()

if error_msg:
    st.error(error_msg)
else:
    gdf, df_pvi, df_news = data_tuple

    # 데이터 병합 준비
    map_name_col = 'SGG_NM' if 'SGG_NM' in gdf.columns else gdf.columns[0]
    gdf['join_key'] = gdf[map_name_col].apply(normalize_region_name)
    
    # PVI 데이터 지역명 정규화
    pvi_name_col = '도시' if '도시' in df_pvi.columns else df_pvi.columns[0]
    df_pvi['join_key'] = df_pvi[pvi_name_col].apply(normalize_region_name)
    
    # 뉴스 데이터 지역명 정규화
    news_name_col = 'region' if 'region' in df_news.columns else df_news.columns[0]
    df_news['join_key'] = df_news[news_name_col].apply(normalize_region_name)
    # 데이터 병합
    merged = gdf.merge(df_pvi[['join_key', 'PVI_Final']], on='join_key', how='left')
    merged = merged.merge(df_news[['join_key', 'count']], on='join_key', how='left')
    
    merged['PVI_Final'] = merged['PVI_Final'].fillna(0)
    merged['count'] = merged['count'].fillna(0)
    merged = calculate_sii_score(merged, 'count')

    # 임계값 설정
    pvi_median = merged['PVI_Final'].median()
    sii_median = merged['SII_Score'].median()
    
    
    # 사이드바 설정
    with st.sidebar:
        st.header("⚙️ 분석 설정 (0.0 ~ 1.0)")
        
        # 수식 도움말을 위한 텍스트
        pvi_help_text = r"""
        가뭄 취약성 지수 (PVI) 계산식:
        $$
        PVI = (\text{SGI}_{norm} \times W_{exp}) + (\text{Sensitivity}_{norm} \times W_{sens}) + (\text{Lack of AC}_{norm} \times W_{ac})
        $$
        - **SGI**: 정규화된 노출 값
        - **Sensitivity**: 정규화된 민감도 (생활·농업용수)
        - **Lack of AC**: 정규화된 적응능력 부족 (미보급·누수)
        - $\text{W}_{element}$: 각 요소의 가중치
        """
        
        sii_help_text = r"""
        사회적 관심도 지수 (SII) 계산식:
        $$
        \\
        SII_{j} = \frac{K_{j} - min(K)}{max(K) - min(K)} \\
        K_{j} = \log(\text{뉴스 기사의 개수 + 1}) \\
        $$
        - **j**: 지역 인덱스
        """
        # pvi 슬라이더
        pvi_thresh = st.slider(
            "PVI 기준값", 
            0.0, 1.0, float(pvi_median),
            help=pvi_help_text
        )
        # sii 슬라이더
        sii_thresh = st.slider(
            "SII 기준값", 
            0.0, 1.0, float(sii_median),
            help=sii_help_text
        )
        
        st.divider()
        st.write(f"📊 PVI 중앙값: {pvi_median:.3f}")
        st.write(f"📊 SII 중앙값: {sii_median:.3f}")

    # 카테고리 분류
    def get_category(row):
        is_pvi_high = row['PVI_Final'] >= pvi_thresh
        is_sii_high = row['SII_Score'] >= sii_thresh
        
        if is_pvi_high and is_sii_high:
            return "알려진 위험"
        elif not is_pvi_high and is_sii_high:
            return "관찰 필요"
        elif is_pvi_high and not is_sii_high:
            return "잠재적 위험"
        else:
            return "안전"

    merged['Category'] = merged.apply(get_category, axis=1)

    # 중심점 계산
    temp_gdf = merged.copy().to_crs(epsg=5179)
    temp_gdf['centroid'] = temp_gdf.geometry.centroid
    temp_gdf = temp_gdf.set_geometry('centroid').to_crs(epsg=4326)
    
    merged_points = merged.copy()
    merged_points['lat'] = temp_gdf.geometry.y
    merged_points['lon'] = temp_gdf.geometry.x

    # 지도 시각화
    merged = merged.set_index('join_key')

    color_map = {
        "잠재적 위험": "#FF0000",
        "알려진 위험": "#595959",
        "관찰 필요": "#FF8C00",
        "안전": "#D1D1D1"
    }
    
    category_orders = {"Category": ["잠재적 위험", "알려진 위험", "관찰 필요", "안전"]}
    # 지도 및 레이아웃 설정
    fig = px.choropleth_map(
        merged,
        geojson=merged.geometry,
        locations=merged.index,
        color='Category',
        color_discrete_map=color_map,
        category_orders=category_orders,
        center={"lat": 37.82, "lon": 128.2},
        map_style="white-bg",
        zoom=8.0,
        opacity=1.0,
        title="<b>강원도 가뭄 위험도 지도 (Log-Normalized)</b>",
        custom_data=[merged.index, merged['PVI_Final'], merged['SII_Score'], merged['count'], merged['Category']]
    )

    # 툴팁 디자인 -> 마우스를 올렸을때 나타나는 정보
    fig.update_traces(
        hovertemplate="<br>".join([
            "<b style='font-size:16px'>%{customdata[0]}</b>",
            "────────────────",
            "<b>📌 상태:</b> %{customdata[4]}",
            "<b>💧 가뭄 지수 (PVI):</b> %{customdata[1]:.3f}",
            "<b>📰 사회 관심 (SII):</b> %{customdata[2]:.3f}",
            "<span style='color:gray; font-size:12px'>(관련 뉴스 기사: %{customdata[3]:,}건)</span>",
            "<extra></extra>"
        ])
    )

    # 텍스트 라벨 추가
    fig.add_trace(go.Scattermap(
        lat=merged_points['lat'],
        lon=merged_points['lon'],
        mode='text',
        text=merged_points['join_key'],
        textposition="middle center",
        textfont=dict(size=14, color='black', family="Malgun Gothic", weight='bold'),
        showlegend=False,
        hoverinfo='skip'
    ))

    # 지도 너비를 1000으로 설정
    fig.update_layout(
        margin={"r":0,"t":40,"l":0,"b":0},
        font=dict(color="black", family="Malgun Gothic"),
        legend_title=dict(text="<b>위험도 분류 (필터)</b>", font=dict(size=14, color="black")),
        legend=dict(
            yanchor="top", y=0.98, xanchor="left", x=0.02, 
            bgcolor="rgba(255,255,255,0.95)", bordercolor="Black", borderwidth=1,
            font=dict(size=13, color="black")
        ),
        width=1000,
        height=800,
        autosize=False
    )

    # -------------------------------------------------------------------------
    # 지도 자동 저장 (최초 1회만 생성됨)
    # -------------------------------------------------------------------------
    if 'default_map_saved' not in st.session_state:
        try:
            # 기본값 지도 저장 (중복 실행 방지를 위해 session_state 사용)
            save_filename = "gangwon_drought_map_default.png"
            fig.write_image(save_filename, scale=4)
            print(f"✅ 기본 지도 저장 완료: {save_filename}")
            st.session_state['default_map_saved'] = True
        except Exception as e:
            # kaleido 패키지가 없거나 권한 문제 시 에러 무시
            print(f"⚠️ 지도 저장 실패 (kaleido 설치 필요): {e}")
    # 지도 출력
    st.plotly_chart(
        fig, 
        width='content',
        config={
            'scrollZoom': True, 
            'displayModeBar': True,
            'toImageButtonOptions': {
                'format': 'png',
                'filename': 'gangwon_drought_map_high_res',
                'height': 1600,
                'width': 2000,
                'scale': 4  
            }
        }
    )
    # 데이터프레임 출력
    st.subheader("📋 지역별 상세 데이터")
    st.dataframe(
        merged[[map_name_col, 'PVI_Final', 'count', 'SII_Score', 'Category']]
        .sort_values(by=['PVI_Final'], ascending=False)
        .style.background_gradient(subset=['PVI_Final', 'SII_Score'], cmap='Reds'),
        width='stretch'
    )