#모든 파일은 main branch로
import sys
# import pvi_analyzer 분석
import crawl_news
# import visualizer 시각화.

def main():
    # PVI (물리적 취약성) 분석
    print("-PVI 분석-")
    #pvi_result = pvi_analyzer.run()
    
    #if pvi_result is None:
    #   print("[ERROR] PVI 분석 ")
    #    return

    # SII (사회적 관심도) 크롤링
    print("-SII 크롤링-\n")
    sii_result = crawl_news.run()
    if sii_result is None:
        print("[ERROR] SII 분석 ")
        return

    # [STEP 3] 시각화 및 교차 분석 실행
    print("-시각화 실행-")
    # visualizer.create_maps(pvi_result, sii_result)
    
    print("--complete--")

if __name__ == "__main__":
    main()