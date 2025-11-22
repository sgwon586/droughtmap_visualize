import re
from copy import deepcopy
from multiprocessing.pool import Pool
from time import sleep
from typing import Any, Dict, List, Optional
from urllib.parse import quote
from datetime import datetime

import requests
import ujson as json
from loguru import logger
from pandas import date_range
import pandas as pd
from tqdm import tqdm
from trafilatura import extract, fetch_url
from trafilatura.settings import DEFAULT_CONFIG

#설정
START_DATE = "2025.05.01"
END_DATE = "2025.09.30"
NUM_WORKERS = 6  # 병렬 처리 수
MAX_TRIALS = 3   # 최대 재시도 횟수
SLEEP_TIME = 1.0 # 요청 간 대기 시간

#강원도 18개 시, 군
REGIONS = [
    #"춘천", "속초",
    "강릉",
    #"동해", "원주", "삼척",
    #"홍천", "횡성", "영월", "평창", "정선",
    #"철원", "화천", "양구", "인제", "고성",
    #"양양", "태백"
]

#봇 의심 차단 방지 헤더
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
    "Referer": "https://m.search.naver.com/",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7"
}

TRAFILATURA_CONFIG = deepcopy(DEFAULT_CONFIG)
TRAFILATURA_CONFIG["DEFAULT"]["DOWNLOAD_TIMEOUT"] = "5"
TRAFILATURA_CONFIG["DEFAULT"]["MIN_OUTPUT_SIZE"] = "50"

#본문 내 불필요 내용 삭제
def clean_news_content(text: str) -> str:
    if not text:
        return ""

    text = re.sub(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', '', text)

    text = re.sub(r'\[.*?\]', '', text)
    text = re.sub(r'\(.*?\)', '', text)

    text = re.sub(r'\w+\s*기자\s*=', '', text)
    text = re.sub(r'\w+\s*기자', '', text)

    text = re.sub(r'<저작권자.*?>', '', text)
    text = re.sub(r'무단전재.*', '', text)
    text = re.sub(r'재배포.*금지', '', text)
    text = re.sub(r'Copyrights.*', '', text, flags=re.IGNORECASE)

    text = re.sub(r'\|', '', text)
    text = re.sub(r'많이 본 기사', '', text)
    text = re.sub(r'관련 기사', '', text)
    text = re.sub(r'오늘의 핫뉴스', '', text)
    text = re.sub(r'기사 스크랩', '', text)

    text = re.sub(r'\n+', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()

    return text


def get_article_body(url: str) -> Optional[Dict[str, Any]]:
    try:
        downloaded = fetch_url(url, config=TRAFILATURA_CONFIG)
        if downloaded is None:
            return None
            
        extracted = extract(
            downloaded,
            output_format="json",
            target_language="ko",
            with_metadata=True,
            deduplicate=True,
            config=TRAFILATURA_CONFIG,
        )
        
        if not extracted:
            return None
            
        article = json.loads(extracted)
        raw_text = article.get("text", "")
        
        #정제함수 호출
        cleaned_text = clean_news_content(raw_text)

        if len(cleaned_text) >= 50:
            article["cleaned_text"] = cleaned_text
            article["source_url"] = url
            return article
            
        return None
    except Exception as e:
        # logger.error(f" Error extracting {url}: {e}")
        return None


def crawl_articles_for_region(region: str) -> List[Dict[str, Any]]:
    query = f"{region} 가뭄"
    encoded_query = quote(query)
    dates = date_range(START_DATE, END_DATE, freq="D")
    all_articles = []

    progress_bar = tqdm(total=len(dates), desc=f"📍 {region}", ncols=100)

    for date in dates:
        date_str = date.strftime("%Y%m%d")

        next_url = (
            "https://s.search.naver.com/p/newssearch/3/api/tab/more?"
            f"query={encoded_query}&sort=0&"
            f"nso=so%3Ar%2Cp%3Afrom{date_str}to{date_str}%2Ca%3Aall&ssc=tab.news.all&"
            f"start=1"
        )

        while True:
            num_trials = 0
            response = None
            while num_trials < MAX_TRIALS:
                try:
                    response = requests.get(next_url, headers=HEADERS, timeout=10)
                    response.raise_for_status()
                    break
                except Exception as e:
                    num_trials += 1
                    sleep(SLEEP_TIME)
            
            if response is None:
                break

            try:
                request_result = response.json()
            except Exception:
                break

            if request_result.get("collection") is None:
                break

            next_url = request_result.get("url", "")
            if not next_url:
                break

            script = request_result["collection"][0].get("script", "")
            article_urls = re.findall(r"\"contentHref\":\"(.*?)\"", script)

            if not article_urls:
                break

            # 병렬로 기사 본문 크롤링
            with Pool(NUM_WORKERS) as pool:
                for article in pool.imap_unordered(get_article_body, article_urls):
                    if article:
                        # 결과 리스트에는 지역명과 정제된 본문만 저장
                        all_articles.append({
                            "region": region,
                            "text": article["cleaned_text"]
                        })

            sleep(SLEEP_TIME)

        progress_bar.update(1)

    progress_bar.close()
    return all_articles


if __name__ == "__main__":
    logger.info("강원도 18개 시, 군 가뭄 뉴스 크롤링 시작")

    #요약 정보 담을 리스트
    summary_data = []

    for region in REGIONS:
        logger.info(f"🚗 {region} 지역 수집 시작")
        articles = crawl_articles_for_region(region)

        count = 0
        if articles:
            df = pd.DataFrame(articles)
            
            #중복 제거 (같은 본문 내용은 제거)
            df = df.drop_duplicates(subset=['text'])
            
            #개별 CSV 저장 (컬럼: region, text)
            df = df[["region", "text"]]
            filename = f"가뭄_{region}.csv"
            df.to_csv(filename, index=False, encoding="utf-8-sig")
            
            count = len(df)
            logger.success(f"{region}: {count}개 기사 저장 완료 → {filename}")
        else:
            logger.warning(f"{region}: 수집된 뉴스가 없습니다.")

        #요약 데이터 추가
        summary_data.append({"region": region, "count": count})

    #전체 요약 파일 저장 (컬럼: region, count)
    summary_df = pd.DataFrame(summary_data)
    summary_filename = "강원도_지역별_뉴스갯수.csv"
    summary_df.to_csv(summary_filename, index=False, encoding="utf-8-sig")
    
    logger.success(f"전체 요약 파일 저장 완료 → {summary_filename}")
    logger.success("전체 지역 크롤링 종료")