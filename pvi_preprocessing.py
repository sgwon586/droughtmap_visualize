#담당   : 이승석
#PVI 전처리 (하나로 취합)
import pandas as pd
import os

#지역명을 '춘천', '강릉' 처럼 단순하게 바꿈.
def clean_city_name(name):
    if pd.isna(name): return ""
    name = str(name)
    
    name = name.replace("강원특별자치도 ", "").strip()
    

    target_cities = ['춘천', '원주', '강릉', '동해', '태백', '속초', '삼척', 
                     '철원', '화천', '양구', '인제', '양양', '고성', 
                     '홍천', '횡성', '영월', '평창', '정선']
    
    for city in target_cities:
        if city in name:
            return city
            
    return name[:2] # 매칭 안 되면 앞 2글자만 반환

def load_and_preprocess():
    file_sgi = 'C:\\Users\\home\\Documents\\GitHub\\DroughtMap\\DATA\\강원_SGI_2024.csv'
    file_supply = 'C:\\Users\\home\\Documents\\GitHub\\DroughtMap\\DATA\\2023년 1일1인 급수량 & 보급률.csv'
    file_farm = 'C:\\Users\\home\\Documents\\GitHub\\DroughtMap\\DATA\\강원도_농경지_면적비율(최종).csv'
    file_yusu = 'C:\\Users\\home\\Documents\\GitHub\\DroughtMap\\DATA\\2023년 유수율.csv'

    def safe_read_csv(path):
        if not os.path.exists(path):
            print(f"[ERROR] 파일 없음: {path}")
            return None
        try:
            return pd.read_csv(path, encoding='utf-8')
        except UnicodeDecodeError:
            try:
                return pd.read_csv(path, encoding='cp949')
            except UnicodeDecodeError:
                return pd.read_csv(path, encoding='euc-kr')
    
    df_sgi = safe_read_csv(file_sgi)
    df_supply = safe_read_csv(file_supply)
    df_farm = safe_read_csv(file_farm)
    df_yusu = safe_read_csv(file_yusu)

    # 도시 이름 통일 작업 
    if 'obsrvtnm' in df_sgi.columns:
        df_sgi['도시'] = df_sgi['obsrvtnm'].apply(clean_city_name)
    else:
        # 혹시 컬럼명이 다를 경우를 대비한 안전장치
        print(f"[경고] SGI 파일 컬럼명 확인 필요. (obsrvtnm 없음). 현재 컬럼: {df_sgi.columns}")
        return None
    
    # clean_city_name 적용해서 단순하게 만듦
    df_supply['도시'] = df_supply['시군별'].apply(clean_city_name)
    df_farm['도시'] = df_farm['시군'].apply(clean_city_name)
    df_yusu['도시'] = df_yusu['구분'].apply(clean_city_name)

    # SGI 데이터 그룹화 (관측소별 -> 도시별 평균값)
    target_col = 'ugrwtrl'  
    df_sgi_grouped = df_sgi.groupby('도시')[target_col].mean().reset_index()
    df_sgi_grouped.rename(columns={target_col: 'SGI'}, inplace=True)

    # 데이터 병합 : 도시 이름마다 합치기
    df_merged = pd.merge(df_sgi_grouped, df_supply, on='도시', how='inner')
    df_merged = pd.merge(df_merged, df_farm, on='도시', how='inner')
    df_merged = pd.merge(df_merged, df_yusu, on='도시', how='inner')

    # 정리
    df_final = pd.DataFrame()
    df_final['도시'] = df_merged['도시']
    df_final['SGI'] = df_merged['SGI']
    df_final['생활용수'] = df_merged['1일1인당급수량']
    df_final['농경지'] = df_merged['농경지비율(%)']
    df_final['보급률'] = df_merged['보급률']
    df_final['유수율'] = df_merged['유수율']

    # 저장
    save_path = 'data/pvi_input_data.csv'
    df_final.to_csv(save_path, index=False, encoding='utf-8-sig')

    print(f"완료")
    return df_final

if __name__ == "__main__":
    load_and_preprocess()