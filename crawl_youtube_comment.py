import os
import sys
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from wordcloud import WordCloud
from collections import Counter
from konlpy.tag import Okt

#유튜브 API키 로드
load_dotenv()
api_key = os.getenv("YOUTUBE_API_KEY")

#API키 없으면 프로그램 종료
if not api_key:
    print("오류: .env 파일에서 API 키를 찾을 수 없습니다.")
    sys.exit()

youtube = build("youtube", "v3", developerKey=api_key)

#검색어를 받아 유튜브에서 조회수 순으로 5개의 영상 찾는 함수
def search_youtube_videos(query, max_results=5):
    try:
        request = youtube.search().list(
            q=query,
            part="snippet",
            maxResults=max_results,
            order="viewCount", #조회수 순
            type="video"
        )
        response = request.execute()
        videos = []
        for item in response["items"]:
            video_id = item["id"]["videoId"]
            title = item["snippet"]["title"]
            videos.append((video_id, title))
        return videos
    
    except HttpError as e:
        print(f"검색 중 오류 발생: {e}")
        return []

#영상 ID 받아서 30개 댓글 수집
def get_video_comments(video_id, max_results=30):
    comments = []
    try:
        request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=max_results,
            textFormat="plainText"
        )
        response = request.execute()
        for item in response.get("items", []):
            comment = item["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
            comments.append(comment)

    #댓글 막힌 영상 등 오류 처리
    except HttpError as e:
        print(f"-> (영상 ID: {video_id}) 댓글 가져오기 오류: {e}")
            
    return comments


print("유튜브 데이터 수집 중...")

query = "강릉 가뭄"
videos = search_youtube_videos(query, max_results=5)

#모든 댓글 담을 문자열 변수
all_comments_text = ""

for vid, title in videos:
    print(f"-- 영상 분석 중: {title}")
    comments = get_video_comments(vid, 20)
    
    if comments:
        #가져온 댓글 리스트 하나의 긴 문자열로 합침
        for c in comments:
            all_comments_text += c + " " 
    else:
        print("댓글 가져올 수 없음")

print(f"\n총 수집된 텍스트 길이: {len(all_comments_text)} 자")

if len(all_comments_text) == 0:
    print("수집된 댓글이 없어 분석을 종료합니다.")
    sys.exit()

#데이터 시각화
print("데이터 분석 및 시각화 시작...")

#폰트 설정 (본인 폰트 경로로 변경해주세요!!)
font_path = "C:/WINDOWS/Fonts/NanumGothic.ttf"
plt.rc('font', family='NanumGothic')

#마이너스(-) 기호 깨짐 방지
plt.rcParams['axes.unicode_minus'] = False

#형태소 분석기로 명사만 추출
okt = Okt()
nouns = okt.nouns(all_comments_text)

#불용어 처리
stop_words = ['강릉', '시민', '가뭄', '영상', '댓글', '뉴스', '진짜', '그냥', '우리', '이형', '생각', '정도', '지금', '보고', '때문', '보겸'] 

#한글자 단어 제외
clean_nouns = [n for n in nouns if len(n) > 1 and n not in stop_words]

#가장 많이 나온 단어 15개 
count = Counter(clean_nouns)
top_nouns = dict(count.most_common(15))

#시각화
plt.figure(figsize=(16, 8))

#워드클라우드
wc = WordCloud(font_path=font_path, background_color='white', width=800, height=600, colormap='tab10')
wc.generate_from_frequencies(top_nouns)

plt.subplot(1, 2, 1)
plt.imshow(wc, interpolation='bilinear')
plt.axis('off') #x축 눈금 삭제
plt.title(f"'{query}' 관련 유튜브 여론 워드클라우드", fontsize=15)

#막대 그래프
plt.subplot(1, 2, 2)
sorted_keys = list(top_nouns.keys())
sorted_values = list(top_nouns.values())

bars = plt.barh(sorted_keys, sorted_values, color='skyblue')
plt.gca().invert_yaxis() #상위 키워드가 맨 위로 오게

#막대 옆 숫자 표시
for bar in bars:
    width = bar.get_width()
    plt.text(width + 0.1, bar.get_y() + bar.get_height()/2, f'{int(width)}회', va='center')

plt.title('핵심 키워드 언급 빈도 (Top 15)', fontsize=15)
plt.xlabel('언급 횟수')
plt.grid(axis='x', linestyle='--', alpha=0.5) #배경 눈금선

plt.tight_layout()
plt.show()

print("시각화 완료 되었습니다.")