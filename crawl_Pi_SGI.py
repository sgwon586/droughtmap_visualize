# 담당 : 승석
# 강원도 18개 시/군 대상 SGI 지하수관측소 데이터 크롤링
import requests
import pandas as pd
import time
import os

# API 설정
SERVICE_KEY = "73a6b2d6ced51612e8f0f2ff64df499ca9cb8f3fb7457794364b6b00002281cb"
URL = 'http://apis.data.go.kr/B500001/drghtSGIIdex_20211020/operInfoList_20211020'

# 수집 기간
START_DATE = '20240501'
END_DATE = '20240930'

# 관측소 목록 파일 경로 
STATION_FILE = 'C:\\Users\\home\\Documents\\GitHub\\DroughtMap\\DATA\\SGI_station_list.csv'

# 저장할 파일 경로
SAVE_PATH = 'C:\\Users\\home\\Documents\\GitHub\\DroughtMap\\DATA\\강원_SGI_2024.csv'

# 수집 대상 도시
TARGET_CITIES = [
    '춘천', '원주', '강릉', '동해', '태백', '속초', '삼척', 
    '철원', '화천', '양구', '인제', '양양', '고성', 
    '홍천', '횡성', '영월', '평창', '정선'
]

   # SGI 관측소 목록 파일 로드 및 데이터 수집
def collect_sgi_data():

    print("- [SGI] 데이터 수집 시작 -")

    # 1. 관측소 목록 파일 확인 및 로드
    if not os.path.exists(STATION_FILE):
        print(f"[오류] '{STATION_FILE}' 파일 없음.")
        return None
    
    try:
        df_stations = pd.read_csv(STATION_FILE)
    except Exception as e:
        print(f"[오류]: {e}")
        return None

    all_sgi_data = [] 

    # 2. 도시별 반복 수집 (중복일수도 있어서.)
    for city in TARGET_CITIES:
        # 해당 도시 이름이 포함된 관측소 찾기
        matched_rows = df_stations[df_stations['cdnm'].str.contains(city, na=False)]
        
        if matched_rows.empty:
            print(f"[{city}] 관측소 없음 -> 건너뜀")
            continue
        
        print(f"[{city}] {len(matched_rows)}개 관측소 수집", end=" ")

        # 3. 발견된 관측소별 API 요청
        for index, row in matched_rows.iterrows():
            station_code = str(row['cd'])
            station_name = row['cdnm']
            
            params = {
                'serviceKey': SERVICE_KEY,
                'pageNo': '1',
                'numOfRows': '366', # 넉넉하게
                'obsrvtCd': station_code,
                'stDt': START_DATE,
                'edDt': END_DATE,
                '_type': 'json'
            }
            
            try:
                response = requests.get(URL, params=params)
                
                if response.status_code == 200:
                    try:
                        data_json = response.json()
                        # 응답 코드 확인 (정상일 경우 '00')
                        header_code = data_json.get('response', {}).get('header', {}).get('resultCode')
                        
                        if header_code == '00':
                            body = data_json['response']['body']
                            if body['totalCount'] > 0:
                                items = body['items']['item']
                                if isinstance(items, dict):
                                    items = [items]                            
                                    
                                all_sgi_data.extend(items)
                    except:
                        pass # JSON 파싱 에러 무시
            except Exception:
                pass # 통신 에러 무시
            
            time.sleep(0.1) # 서버 부하 방지
        
        print("완료")

    # 4. 결과
    if all_sgi_data:
        df_result = pd.DataFrame(all_sgi_data)       
        
        df_result.to_csv(SAVE_PATH, index=False, encoding='utf-8-sig')
        print(f"\n[성공]")
        return df_result
    else:
        print("\n[실패]")
        return None

if __name__ == "__main__":
    collect_sgi_data()