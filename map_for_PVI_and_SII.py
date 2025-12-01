import geopandas as gpd
import pandas as pd
import os
import plotly.express as px
import plotly.graph_objects as go
import numpy as np

# 현재 스크립트 위치 기준 경로 설정
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# =============================================================================
# [1] 데이터 로드 및 전처리
# =============================================================================
def load_and_process_data():
    print("📂 데이터 로드 및 전처리 중...")
    
    # 1. 지도 파일 찾기
    map_files = ["gangwon_map_simplified.parquet", "processed_gangwon_analaysis.parquet"]
    map_path = None
    for f in map_files:
        full_path = os.path.join(BASE_DIR, f)
        if os.path.exists(full_path):
            map_path = full_path
            break
            
    if not map_path:
        raise FileNotFoundError("❌ 지도 파일(.parquet)을 찾을 수 없습니다.")

    gdf = gpd.read_parquet(map_path)
    if gdf.crs != "epsg:4326":
        gdf = gdf.to_crs(epsg=4326)

    # 2. CSV 데이터 로드
    data_dir = os.path.join(BASE_DIR, "DATA")
    pvi_path = os.path.join(data_dir, "pvi_result_final.csv")
    news_path = os.path.join(data_dir, "강원도_지역별_뉴스갯수.csv")

    if not os.path.exists(pvi_path) or not os.path.exists(news_path):
        raise FileNotFoundError(f"❌ CSV 파일을 찾을 수 없습니다. ({data_dir})")

    try: df_pvi = pd.read_csv(pvi_path, encoding='utf-8')
    except: df_pvi = pd.read_csv(pvi_path, encoding='cp949')

    try: df_news = pd.read_csv(news_path, encoding='euc-kr')
    except:
        try: df_news = pd.read_csv(news_path, encoding='utf-8')
        except: df_news = pd.read_csv(news_path, encoding='cp949')

    # 3. 데이터 병합
    def normalize_region_name(name):
        if pd.isna(name): return ""
        name = str(name).strip()
        name = name.replace("강원특별자치도", "").replace("강원도", "").strip()
        if len(name) > 1:
            if name.endswith("시") or name.endswith("군"):
                return name[:-1]
        return name

    map_name_col = 'SGG_NM' if 'SGG_NM' in gdf.columns else gdf.columns[0]
    gdf['join_key'] = gdf[map_name_col].apply(normalize_region_name)
    
    pvi_name_col = '도시' if '도시' in df_pvi.columns else df_pvi.columns[0]
    df_pvi['join_key'] = df_pvi[pvi_name_col].apply(normalize_region_name)
    
    news_name_col = 'region' if 'region' in df_news.columns else df_news.columns[0]
    df_news['join_key'] = df_news[news_name_col].apply(normalize_region_name)

    merged = gdf.merge(df_pvi[['join_key', 'PVI_Final']], on='join_key', how='left')
    merged = merged.merge(df_news[['join_key', 'count']], on='join_key', how='left')
    
    merged['PVI_Final'] = merged['PVI_Final'].fillna(0)
    merged['count'] = merged['count'].fillna(0)

    # 4. SII 점수 계산 (로그 정규화)
    merged['log_val'] = np.log1p(merged['count'])
    min_val = merged['log_val'].min()
    max_val = merged['log_val'].max()
    merged['SII_Score'] = (merged['log_val'] - min_val) / (max_val - min_val) if (max_val - min_val) != 0 else 0.0

    return merged

# =============================================================================
# [2] 지도 생성 및 저장
# =============================================================================
def generate_maps(merged_df):
    print("🎨 지도 이미지 생성 시작...")

    # 공통 설정
    map_center = {"lat": 37.82, "lon": 128.2}
    map_zoom = 7.7
    common_layout = dict(
        margin={"r":0,"t":40,"l":0,"b":0},
        font=dict(color="black", family="Malgun Gothic"),
        width=1000,
        height=800,
        autosize=False,
        coloraxis_showscale=True
    )

    # 지역명 라벨 생성
    temp_gdf = merged_df.copy().to_crs(epsg=5179)
    temp_gdf['centroid'] = temp_gdf.geometry.centroid
    temp_gdf = temp_gdf.set_geometry('centroid').to_crs(epsg=4326)
    
    label_trace = go.Scattermap(
        lat=temp_gdf.geometry.y,
        lon=temp_gdf.geometry.x,
        mode='text',
        text=merged_df['join_key'],
        textposition="middle center",
        textfont=dict(size=14, color='black', family="Malgun Gothic", weight='bold'),
        showlegend=False,
        hoverinfo='skip'
    )

    # -------------------------------------------------------------------------
    # 1. PVI 지도 (White -> Red)
    # -------------------------------------------------------------------------
    print("  -> [1/2] PVI 지도(빨강) 생성 중...")
    fig_pvi = px.choropleth_map(
        merged_df,
        geojson=merged_df.geometry,
        locations=merged_df.index,
        color='PVI_Final',
        color_continuous_scale=['#FFFFFF', '#FF0000'], # 흰색 -> 빨강
        range_color=[0, 1],
        center=map_center, zoom=map_zoom,
        map_style="white-bg", opacity=1.0,
        title="<b>강원도 가뭄 취약성 지수 (PVI)</b>"
    )
    fig_pvi.add_trace(label_trace)
    fig_pvi.update_layout(**common_layout)
    
    try:
        fig_pvi.write_image("map_pvi_red.png", scale=2)
        print("     ✅ 저장 완료: map_pvi_red.png")
    except Exception as e:
        print(f"     ⚠️ 저장 실패: {e}")

    # -------------------------------------------------------------------------
    # 2. SII 지도 (White -> Blue)
    # -------------------------------------------------------------------------
    print("  -> [2/2] SII 지도(파랑) 생성 중...")
    fig_sii = px.choropleth_map(
        merged_df,
        geojson=merged_df.geometry,
        locations=merged_df.index,
        color='SII_Score',
        color_continuous_scale=['#FFFFFF', '#0000FF'], # 흰색 -> 파랑
        range_color=[0, 1],
        center=map_center, zoom=map_zoom,
        map_style="white-bg", opacity=1.0,
        title="<b>강원도 사회적 관심도 지수 (SII)</b>"
    )
    fig_sii.add_trace(label_trace)
    fig_sii.update_layout(**common_layout)

    try:
        fig_sii.write_image("map_sii_blue.png", scale=2)
        print("     ✅ 저장 완료: map_sii_blue.png")
    except Exception as e:
        print(f"     ⚠️ 저장 실패: {e}")

    print("\n🎉 모든 작업이 완료되었습니다.")

# =============================================================================
# [3] 메인 실행
# =============================================================================
if __name__ == "__main__":
    try:
        data = load_and_process_data()
        generate_maps(data)
    except Exception as e:
        print(f"\n[오류 발생] {e}")