import requests
import pandas as pd
from time import sleep
from loguru import logger



# 설정값
API_KEY = "N2FiNGVjOGJhYjU5OGEyOWM4NWQ3Mzk0M2ZkZDRjMTM="
YEAR = "2023"
OUTPUT_PATH =  r"DATA\2023년 1일1인 급수량 & 보급률.csv"

MAX_TRIALS = 3
SLEEP_TIME = 0.5



def build_kosis_url(api_key: str, start_year: str, end_year: str) -> str:
    itm_ids = (
        "1621113103117118T1+"
        "1621113103117118T2+"
        "1621113103117118T3+"
        "1621113103117118T4+"
        "1621113103117118T5+"
        "1621113103117118T6+"
        "162111621113103117118T8+"
    )

    url = (
        "https://kosis.kr/openapi/Param/statisticsParameterData.do?"
        f"method=getList&apiKey={api_key}&itmId={itm_ids}"
        "&objL1=ALL&objL2=&objL3=&objL4=&objL5=&objL6=&objL7=&objL8="
        "&format=json&jsonVD=Y&prdSe=Y"
        f"&startPrdDe={start_year}&endPrdDe={end_year}"
        "&outputFields=ORG_ID+TBL_ID+TBL_NM+OBJ_ID+OBJ_NM+OBJ_NM_ENG+"
        "NM+NM_ENG+ITM_ID+ITM_NM+ITM_NM_ENG+UNIT_NM+UNIT_NM_ENG+"
        "PRD_SE+PRD_DE+LST_CHN_DE+"
        "&orgId=211&tblId=DT_211002_G008"
    )

    return url


def fetch_kosis_data(url: str, max_trials: int = 3, sleep_time: float = 0.5):
    num_trials = 0

    while num_trials < max_trials:
        try:
            response = requests.get(url)
            return response.json()
        except Exception:
            logger.warning(f"요청 실패. 재시도 ({num_trials + 1}/{max_trials})")
            num_trials += 1
            sleep(sleep_time)

    logger.error("API 요청 횟수 초과")
    return []


def kosis_data(data, year: str) -> pd.DataFrame:
    df = pd.DataFrame(data)

    df = df[df["PRD_DE"] == year]
    df = df[df["C1_NM"] != "전체"]
    df["시군별"] = df["C1_NM"]

    person = df[df["ITM_ID"] == "1621113103117118T6"][["시군별", "DT"]]
    person = person.rename(columns={"DT": "1일1인당급수량"})

    penetration = df[df["ITM_ID"] == "1621113103117118T3"][["시군별", "DT"]]
    penetration = penetration.rename(columns={"DT": "보급률"})

    df_2023 = pd.merge(person, penetration, on="시군별")

    return df_2023


def main():
    print("KOSIS API 데이터 로딩 중")

    url = build_kosis_url(API_KEY, YEAR, YEAR)
    data = fetch_kosis_data(url, MAX_TRIALS, SLEEP_TIME)

    if not data:
        print("데이터를 로딩 실패 ")
        return

    df = kosis_data(data, YEAR)
    df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")

    print(f"CSV 저장 완료: {OUTPUT_PATH}")
    print(df.head())


if __name__ == "__main__":
    main()
