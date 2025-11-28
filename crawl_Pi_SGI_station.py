import requests
import pandas as pd
import os

# API 설정
SERVICE_KEY = "73a6b2d6ced51612e8f0f2ff64df499ca9cb8f3fb7457794364b6b00002281cb"
URL = 'https://apis.data.go.kr/B500001/drought/drghtFcltyCode/fcltyList'

# 저장할 파일 경로
SAVE_PATH = 'C:\\Users\\home\\Documents\\GitHub\\DroughtMap\\DATA\\SGI_station_list.csv'

# 강원도 18개 시/군 검색 키워드
TARGET_CITIES = [
    '춘천', '원주', '강릉', '동해', '태백', '속초', '삼척', 
    '철원', '화천', '양구', '인제', '양양', '고성', 
    '홍천', '횡성', '영월', '평창', '정선'
]

def create_station_list():
    print("--- [관측소 목록] 생성 시작 ---")
    
    # 1. API 요청 
    params = {
        'ServiceKey': SERVICE_KEY,
        'pageNo': '1',
        'numOfRows': '3000', 
        '_type': 'json',
        'fcltyDivCode': '1007' # 1007: 지하수관측소 (SGI 산출용)
    }
    
    try:
        response = requests.get(URL, params=params)
        #error handling
        if response.status_code != 200:
            print(f"[오류] API 호출 실패: {response.status_code}")
            return

        data = response.json()
        items = data['response']['body']['items']['item']
        
        # 2. 데이터프레임 변환
        df = pd.DataFrame(items)
        
        # 3. 강원도 18개 시/군 필터링
        # (관측소 이름 'cdnm'에 도시 이름이 포함된 것만 남김)
        search_pattern = '|'.join(TARGET_CITIES)
        df_gangwon = df[df['cdnm'].str.contains(search_pattern, na=False)].copy()
        
        # 4. 필요한 컬럼만 선택 ('cd': 코드, 'cdnm': 이름)
        df_final = df_gangwon[['cd', 'cdnm']]
        
        # 5. 저장
        # 추출후 포항동해, 고성우산, 고성거류 직접 제거.
        if not os.path.exists('data'):
            os.makedirs('data')
            
        df_final.to_csv(SAVE_PATH, index=False, encoding='utf-8-sig')
        print(f"[완료]")
        
    except Exception as e:
        print(f"[오류]: {e}")

if __name__ == "__main__":
    create_station_list()