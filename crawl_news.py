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
END_DATE = "2025.10.31"
NUM_WORKERS = 6 # 병렬 처리 수
MAX_TRIALS = 3 # 최대 재시도 횟수
SLEEP_TIME = 1.0 # 요청 간 대기 시간

# 강원도 18개 시, 군
REGIONS = [
    "춘천", "속초", "강릉",
    #"동해", "원주", "삼척",
    #"홍천", "횡성", "영월", "평창", "정선",
    #"철원", "화천", "양구", "인제", "고성",
    #"양양", "태백"
]


TRAFILATURA_CONFIG = deepcopy(DEFAULT_CONFIG)
TRAFILATURA_CONFIG["DEFAULT"]["DOWNLOAD_TIMEOUT"] = "5" #최대 5초까지만 대기
TRAFILATURA_CONFIG["DEFAULT"]["MIN_OUTPUT_SIZE"] = "50" #본문이 50자 이상인 경우만


# 뉴스 본문 추출 함수
def get_article_body(url: str) -> Optional[Dict[str, Any]]:
    """주어진 기사 URL에서 본문 추출"""
    try:
        downloaded = fetch_url(url, config=TRAFILATURA_CONFIG)
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
        if "text" in article and len(article["text"]) >= 50:
            article["source_url"] = url
            return article
        return None
    except Exception as e:
        logger.error(f" Error extracting {url}: {e}")
        return None


# 날짜별 뉴스 수집 함수
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
            while num_trials < MAX_TRIALS:
                try:
                    response = requests.get(next_url, timeout=10)
                    break
                except Exception as e:
                    num_trials += 1
                    logger.warning(f"Retrying {next_url} ({num_trials}/{MAX_TRIALS}) due to {e}")
                    sleep(SLEEP_TIME)
            else:
                logger.error(f"Failed to fetch data for {date_str} after {MAX_TRIALS} retries")
                break

            try:
                request_result = response.json()
            except Exception:
                logger.warning(f"Invalid JSON on {date_str}")
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
                        article["region"] = region
                        article["date_crawled"] = date_str
                        all_articles.append(article)

            sleep(SLEEP_TIME)

        progress_bar.update(1)

    progress_bar.close()
    return all_articles


# 메인 함수
if __name__ == "__main__":
    logger.info("강원도 18개 시, 군 가뭄 뉴스 크롤링 시작")

    for region in REGIONS:
        logger.info(f"🚗 {region} 지역 수집 시작")
        articles = crawl_articles_for_region(region)

        if not articles:
            logger.warning(f"{region}: 수집된 뉴스가 없습니다.")
            continue

        df = pd.DataFrame(articles)
        df = df[["region", "title", "author", "date", "text", "source_url"]]

        filename = f"가뭄_{region}.csv"
        df.to_csv(filename, index=False, encoding="utf-8-sig")
        logger.success(f"😊 {region}: {len(df)}개 기사 저장 완료 → {filename}")

    logger.success("전체 지역 크롤링 완료")
