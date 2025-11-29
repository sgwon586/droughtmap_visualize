import geopandas as gpd
import pandas as pd
import glob
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def load_map_data():
    """map 폴더의 Shapefile들을 읽어와 하나로 병합"""
    print("1. 지도 데이터 로드 중...")
    map_pattern = os.path.join(BASE_DIR, "map", "*.shp")
    map_files = glob.glob(map_pattern)
    
    if not map_files:
        print(f"  [Error] 지도 파일을 찾을 수 없습니다: {map_pattern}")
        return None

    gdfs = []
    for f in map_files:
        try:
            # 인코딩 처리 -> 어떤 인코딩 방식인지 모를때를 대비하기 위함
            try:
                gdf = gpd.read_file(f, encoding='utf-8')
            except UnicodeDecodeError:
                gdf = gpd.read_file(f, encoding='cp949')

            # 좌표계가 없으면 기본값(EPSG:5179) 설정
            if gdf.crs is None: 
                gdf.set_crs("epsg:5179", inplace=True)
            
            gdfs.append(gdf)
        except Exception as e:
            print(f"  지도 로드 실패 ({f}): {e}")

    if not gdfs: return None
    
    # 여러 구역의 Shapefile을 하나로 합침
    kw_map = pd.concat(gdfs, ignore_index=True)
    return kw_map

def process_map():
    # 1. 지도 데이터 로드
    kw_map = load_map_data()
    if kw_map is None: return

    print("2. 지도 데이터 정리 (행정구역 병합)...")
    
    # 필요한 컬럼만 남기고 행정구역 코드(sgg_cd) 기준으로 병합 (쪼개진 폴리곤 합치기)
    if 'sgg_cd' in kw_map.columns:
        cols_to_keep = ['sgg_cd', 'SGG_NM', 'geometry']
        valid_cols = [c for c in cols_to_keep if c in kw_map.columns]
        
        temp_df = kw_map[valid_cols].copy()
        # 중복 컬럼 제거
        temp_df = temp_df.loc[:, ~temp_df.columns.duplicated()]
        
        # dissolve: 같은 행정구역 코드를 가진 도형들을 하나로 합침
        kw_map = temp_df.dissolve(by='sgg_cd', as_index=False, aggfunc='first')
        print(f"  -> 병합 완료. 총 {len(kw_map)}개 행정구역.")

    # 3. 좌표계 변환 및 단순화
    print("3. 좌표계 변환 및 형상 단순화...")
    final_gdf = kw_map.copy()

    # 웹 지도 등에서 많이 쓰는 경위도 좌표계(EPSG:4326)로 변환
    final_gdf = final_gdf.to_crs(epsg=4326)
    
    # 지형 단순화 -> 지도 파일 크기 감소, 
    final_gdf['geometry'] = final_gdf.geometry.simplify(tolerance=0.001)

    # 4. Parquet 파일로 저장
    output_filename = "gangwon_map_simplified.parquet"
    final_gdf.to_parquet(output_filename)
    print(f"완료: '{output_filename}' 파일이 생성되었습니다.")

if __name__ == "__main__":
    process_map()