import pandas as pd

# 파일 경로
file_path = r"C:\Users\82106\OneDrive\바탕 화면\2023년 유수율.csv"
OUTPUT_PATH =  r"DATA\2023년 유수율.csv"

# 파일 로드
df = pd.read_csv(file_path , encoding="cp949")


# 강원도 18개 시·군
gangwon_list = [
    "강원특별자치도 춘천시","강원특별자치도 원주시","강원특별자치도 강릉시","강원특별자치도 동해시","강원특별자치도 태백시","강원특별자치도 속초시","강원특별자치도 삼척시",
    "강원특별자치도 홍천군","강원특별자치도 횡성군","강원특별자치도 영월군","강원특별자치도 평창군","강원특별자치도 정선군",
    "강원특별자치도 철원군","강원특별자치도 화천군","강원특별자치도 양구군","강원특별자치도 인제군","강원특별자치도 고성군","강원특별자치도 양양군"
]

# 파일의 '구분' 컬럼을 기준으로 필터링
filtered = df[df["구분"].isin(gangwon_list)]

# 지역명과 유수율 추출 
filtered = filtered[["구분", "유수율"]]

# CSV로 저장
filtered.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")

print("저장 완료: 2023년 유수율.csv")
