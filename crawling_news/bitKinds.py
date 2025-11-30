# https://www.bigkinds.or.kr 자동 크롤링 모듈 

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
import datetime
import time
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import TimeoutException
from langchain_core.documents import Document
import platform
import schedule
import re
import json


# Selenium WebDriver 초기화

def init_driver() -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    # # [리눅스 배포 시 필수 설정]
    # options.add_argument("--headless")       # 창 없이 실행
    # options.add_argument("--no-sandbox")     # 리눅스 보안 권한 문제 방지
    # options.add_argument("--disable-dev-shm-usage") # 메모리 공유 문제 방지

    driver = webdriver.Chrome(options=options)
    url = "https://www.bigkinds.or.kr/"
    driver.get(url)
    return driver


# 메인 페이지 팝업창 2개 처리 함수
def popup_handling(driver: webdriver.Chrome, wait: WebDriverWait) -> None:
    try:
        popup1_close_xpath = '//*[@id="popup-dialog-127"]/div/div[2]/div/div[2]/button'
        close_button1 = wait.until(EC.element_to_be_clickable((By.XPATH, popup1_close_xpath)))
        close_button1.click()
        print("첫 번째 팝업을 닫았습니다.")
    except Exception as e:
        print(f"첫 번째 팝업 닫기 실패: {e}")
    try:
        popup2_close_xpath = '//*[@id="popup-dialog-128"]/div/div[2]/div/div[2]/button'
        close_button2 = wait.until(EC.element_to_be_clickable((By.XPATH, popup2_close_xpath)))
        close_button2.click()
        print("두 번째 팝업을 닫았습니다.")
    except Exception as e:
        print(f"두 번째 팝업 닫기 실패: {e}")

# 검색 키워드 입력 및 필터 설정
def search_keyword(driver: webdriver.Chrome, wait: WebDriverWait) -> None:
    # 오늘날짜일형식 20251116
    # 2025-11-16 형식으로 변환
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    # today = "2025-11-28"  # 테스트용 고정 날짜 (나중에 지우기)!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    # //*[@id="ig-sd-btn"] xpath 클릭
    search_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="ig-sd-btn"]')))
    search_button.click()
    print("검색 버튼을 클릭했습니다.")
    # //*[@id="ds-modal"]/div[3]/div/div[2]/ul/li[2]/a xpath 클릭
    search_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="ds-modal"]/div[3]/div/div[2]/ul/li[2]/a')))
    search_button.click()
    print("검색 버튼을 클릭했습니다.")
    # //*[@id="search-begin-date"] 클릭해서 안에 내용 지우고 오늘날짜 2025-11-16 입력
    search_begin_date = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="search-begin-date"]')))

    search_end_date = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="search-end-date"]')))

    # 맥일때 내용 지우기

    if platform.system() == "Darwin":
        try:
            search_begin_date.send_keys(Keys.COMMAND + "A")  # 키보드 명령어 컨트롤 조합 명령어 맥일때 한글도 포함 
            search_begin_date.send_keys(Keys.DELETE)
            search_begin_date.send_keys(today)
            search_begin_date.send_keys(Keys.ENTER)
            print(f"오늘날짜 {today} 입력했습니다.")

            search_end_date.send_keys(Keys.COMMAND + "A")  # 키보드 명령어 컨트롤 조합 명령어 맥일때 한글도 포함
            search_end_date.send_keys(Keys.DELETE)
            search_end_date.send_keys(today)
            search_end_date.send_keys(Keys.ENTER)
        except Exception as e:
            print(f"오류: {e}")

    # 키보드 명령어 컨트롤 조합 명령어 윈도우일때 
    if platform.system() == "Windows":
        try:
            search_begin_date.send_keys(Keys.CONTROL + "a")
            search_begin_date.send_keys(Keys.DELETE)
            search_begin_date.send_keys(today)
            search_begin_date.send_keys(Keys.ENTER)
            print(f"오늘날짜 {today} 입력했습니다.")
        except Exception as e:
            print(f"오류: {e}")

    # 내용 지우기 버튼 클릭

    ############################################# 언론사 선택 ##############################################################
    # //*[@id="ds-modal"]/div[3]/div/div[2]/ul/li[3]/a xpath 클릭
    search_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="ds-modal"]/div[3]/div/div[2]/ul/li[3]/a')))
    search_button.click()
    print("언론사 선택 버튼을 클릭했습니다.")
    # //*[@id="categoryProviderGroup"]/li[3]/label/div/svg 클릭
    search_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="categoryProviderGroup"]/li[3]/label/div')))
    search_button.click()
    print("언론사 선택 버튼을 클릭했습니다.")
    # //*[@id="ds-modal"]/div[1]/div[2]/label 클릭
    search_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="ds-modal"]/div[1]/div[2]/label')))
    search_button.click()
    print("지역일간지 경기를 선택 버튼을 클릭했습니다.")
    # 바디 클릭 호버 초기화

    driver.find_element(By.XPATH, '//*[@id="srch-tab2"]/div[1]/div/p').click()    
    
    # #### 지역주간지  수정 로직 ##############################################################
    # //*[@id="categoryProviderList"]/div[74]/label 클릭
    # //*[@id="categoryProviderGroup"]/li[4]/label/div
    search_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="categoryProviderGroup"]/li[4]/label/div')))
    search_button.click()
    print("지역주간지 경기를 선택 버튼을 클릭했습니다.")
    # //*[@id="ds-modal"]/div[2]/div[2]/label 클릭
    search_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="ds-modal"]/div[2]/div[2]/label')))
    search_button.click()
    print("경기 지역주간지 선택 버튼을 클릭했습니다.")

    ############################################# 통합 분류란 선택 ##############################################################
    # //*[@id="ds-modal"]/div[3]/div/div[2]/ul/li[4]/a 클릭
    search_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="ds-modal"]/div[3]/div/div[2]/ul/li[4]/a')))
    search_button.click()
    print("통합분류란을 선택 버튼을 클릭했습니다.")
    # //*[@id="srch-tab3"]/ul/li[2]/div/span[2] 클릭
    search_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="srch-tab3"]/ul/li[2]/div/span[2]')))
    search_button.click()
    print("경제 선택 버튼을 클릭했습니다.")
    # //*[@id="srch-tab3"]/ul/li[2]/ul/li[2]/div/span[3]/label/span 클릭
    search_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="srch-tab3"]/ul/li[2]/ul/li[2]/div/span[3]/label/span')))
    search_button.click()
    print("부동산 선택 버튼을 클릭했습니다.")

    # //*[@id="ds-modal"]/div[3]/div/div[2]/ul/li[6]/a 클릭
    search_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="ds-modal"]/div[3]/div/div[2]/ul/li[6]/a')))  
    search_button.click()
    print("상세검색 적용 버튼을 클릭했습니다.")

    # //*[@id="orKeyword1"] 클릭 후 부동산 입력
    search_box = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="orKeyword1"]')))
    search_box.clear()
    search_box.send_keys("부동산")
    print("검색어 '부동산'을 입력했습니다.")

    # //*[@id="ds-modal"]/div[3]/div/div[8]/div[2]/div/button[2] 클릭
    search_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="ds-modal"]/div[3]/div/div[8]/div[2]/div/button[2]')))
    search_button.click()
    print("적용 버튼을 클릭했습니다.")


# ----------------------------중요한 부분: 뉴스 아이템들을 텍스트로 변환하는 함수 ----------------------------
# search 결과에 나온 각각의 뉴스들을 텍스트로 변환
# 반환 타입 지정 list[dict] - 딕셔너리 구조 { "title": 제목, "body": 본문 }
def html_to_text_sel(driver: webdriver.Chrome, wait: WebDriverWait) -> list[dict]:
    s_list = [] # 문자열 딕셔너리 리스트 초기화(뉴스 제목과 본문 저장용)
    # 분석 제외 
    # //*[@id="filterTab05"]/li[1]/span/label 클릭
    # 분석 제외 (수정된 부분)
    # ---------------------------------------------------------------------------
    try:
        # 1. 클릭 가능 여부(clickable) 대신 요소가 존재하는지(presence)만 확인합니다.
        # (화면에 안 보여도 DOM에 있으면 찾아냄)
        exclude_xpath = '//*[@id="filterTab05"]/li[1]/span/label'
        exclude_button = wait.until(EC.presence_of_element_located((By.XPATH, exclude_xpath)))
        
        # 2. Selenium의 물리 클릭(.click()) 대신 JavaScript로 강제 클릭을 실행합니다.
        driver.execute_script("arguments[0].click();", exclude_button)
        
        print("분석 제외 버튼을 클릭했습니다 (JS 실행).")
        time.sleep(0.5) # 클릭 후 잠시 대기
        
    except Exception as e:
        print(f"분석 제외 버튼 클릭 실패: {e}")

    # 1. 뉴스 아이템 로드 시도 (검색 결과가 0건일 경우 여기서 Timeout 발생)
    try:
        # 뉴스 아이템들이 로드될 때까지 대기 (기존 로직)
        print("뉴스 검색 결과를 기다리고 있습니다...")
        news_items = wait.until(EC.presence_of_all_elements_located((By.XPATH, '//*[@id="news-results"]/div/div/div[2]/a/div/strong/span')))
        print(f"현재 페이지의 뉴스 아이템 수: {len(news_items)}")
        
    except TimeoutException:
        # 10초 동안 기다려도 뉴스 아이템이 안 뜨면 검색 결과가 없는 것으로 간주
        print(">>> 검색된 뉴스가 없습니다. (빈 리스트 반환)")
        return []  # 빈 리스트 반환 후 함수 종료

    while True:
        # //*[@id="news-results"]/div[1]/div/div[2]/a/div/strong/span
        # //*[@id="news-results"]/div/div/div[2]/a/div/strong/span 뉴스 아이템들 로드 대기
        # 모든 뉴스 아이템들이 로드될 때까지 대기
        # 3번 재시도 로직 추가
        for attempt in range(3): 
            news_items = wait.until(EC.presence_of_all_elements_located((By.XPATH, '//*[@id="news-results"]/div/div/div[2]/a/div/strong/span'))) # 뉴스 아이템들 로드 대기
            print(f"현재 페이지의 뉴스 아이템 수: {len(news_items)}")
            if news_items:
                break
            else:
                print(f"뉴스 아이템이 로드되지 않았습니다. {attempt + 1}번째 재시도 중")
                time.sleep(2)  # 2초 대기 후 재시도
        else:
            print("뉴스 아이템을 로드하지 못했습니다. 크롤링을 종료합니다.")
            break  # 뉴스 아이템을 로드하지 못했으면 루프 종료

        for index, item in enumerate(news_items):   
            # 각 뉴스 아이템 클릭 후 문자열 수집 
            # 뉴스 아이템 클릭 전까지 대기
            # 뉴스 아이템이 모두 로드될 때까지 대기
            wait.until(EC.presence_of_all_elements_located((By.XPATH, '//*[@id="news-results"]/div/div/div[2]/a/div/strong/span')))
            
            try:
                # 뉴스 아이템 클릭이 가능할때까지 대기 
                clickable_item = wait.until(EC.element_to_be_clickable((By.XPATH, f'//*[@id="news-results"]/div/div/div[2]/a[{index + 1}]/div/strong/span')))
                clickable_item.click()
                print(f"{index + 1}번째 뉴스 아이템을 클릭했습니다.")
                # 뉴스 제목 텍스트 추출 //*[@id="news-detail-modal"]/div/div/div/div[1]/div/div[1]/h1
                title_xpath = '//*[@id="news-detail-modal"]/div/div/div/div[1]/div/div[1]/h1'
                # 뉴스 제목이 비어있지 않을 때까지 대기
                article_title = wait.until(lambda d: d.find_element(By.XPATH, title_xpath) 
                                           if d.find_element(By.XPATH, title_xpath).text.strip() != "" 
                                           else False
                                           )
                print(f"뉴스 제목: {article_title.text}")
                article_title_text = article_title.text
                
                # 기자와 신문사 정보 추출 (선택 사항)
                # //*[@id="news-detail-modal"]/div/div/div/div[1]/div/div[1]/div[1]/ul/li[2] 기자 
                # //*[@id="news-detail-modal"]/div/div/div/div[1]/div/div[1]/div[1]/ul/li[1] 신문사
                # (현재는 사용하지 않음)
                reporter_xpath = '//*[@id="news-detail-modal"]/div/div/div/div[1]/div/div[1]/div[1]/ul/li[2]'
                newspaper_xpath = '//*[@id="chkProviderImage"]/img' # onerror시 
                try:
                    reporter_info = driver.find_element(By.XPATH, reporter_xpath).text
                    newspaper_info = driver.find_element(By.XPATH, newspaper_xpath).text
                    print(f"기자 정보: {reporter_info}, 신문사 정보: {newspaper_info}")
                except Exception as e:
                    print(f"기자 및 신문사 정보 추출 실패 (무시): {e}")

                # 뉴스 본문 텍스트 추출 
                # //*[@id="news-detail-modal"]/div/div/div/div[1]/div/div[2]/div[2] 사진이 있을 경우의 본문
                # //*[@id="news-detail-modal"]/div/div/div/div[1]/div/div[2]/div 사진이 없을 경우의 본문

                body_xpath = '//*[@id="news-detail-modal"]/div/div/div/div[1]/div/div[2]/div[2]' # 사진이 있을 경우의 본문 
                try:
                    # 사진이 있을 경우의 본문 시도
                    article_body = wait.until(lambda d: d.find_element(By.XPATH, body_xpath) 
                                              if d.find_element(By.XPATH, body_xpath).text.strip() != "" 
                                              else False
                                              ) # 본문이 비어있지 않을 때까지 대기
                except Exception as e:
                    # 대체 본문 XPath 시도
                    body_xpath_alt = '//*[@id="news-detail-modal"]/div/div/div/div[1]/div/div[2]/div' # 사진이 없을 경우의 본문
                    article_body = wait.until(lambda d: d.find_element(By.XPATH, body_xpath_alt) 
                                              if d.find_element(By.XPATH, body_xpath_alt).text.strip() != "" 
                                              else False
                                              ) # 본문이 비어있지 않을 때까지 대기

                # --- 공백 정리 로직 (추가) ---
                # 본문 텍스트에서 불필요한 공백 및 줄바꿈 제거
                raw_body_text = article_body.text
                cleaned_body_text = re.sub(r'\s+', ' ', raw_body_text).strip() # \s+는 모든 공백 문자(스페이스, 탭, 줄바꿈 등)를 의미
                
                time.sleep(0.5)  # 페이지 로드 대기
                
                # 뉴스 모달 닫기
                # 모달 닫기 버튼 클릭 //*[@id="news-detail-modal"]/div/div/div/button
                # //*[@id="news-detail-modal"]/div/div/div/button
                close_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="news-detail-modal"]/div/div/div/button')))
                close_button.click()
                print("뉴스 모달을 닫았습니다.")
                
                # 수집한 텍스트를 리스트에 추가
                # 딕셔너리로 저장
                news_dict = {
                    "title": article_title_text,
                    "body": cleaned_body_text
                }
                # 뉴스 딕셔너리 리스트에 저장 
                s_list.append(news_dict)

                # 다음 페이지 로직처리
                try:
                    # 'body' 태그(빈 공간)를 클릭하여 모든 호버 효과를 해제합니다.
                    driver.find_element(By.TAG_NAME, 'body').click()
                    print("빈 공간을 클릭하여 호버 효과를 초기화했습니다.")
                    time.sleep(0.2) # 호버 효과가 사라지는 시간 대기
                except Exception as e:
                    print(f"빈 공간 클릭 실패 (무시): {e}")

                time.sleep(1)  # 페이지 로드 대기
            except Exception as e:
                print(f"뉴스 아이템 처리 중 오류 발생: {e}")
        
        # --- 다음 페이지 로직 ---
        # 더 안정적인 XPath로 '다음 페이지' 버튼을 찾습니다.
        # title 속성을 사용하여 '다음 페이지' 버튼을 명확히 지정
        # //a[contains(@class, 'page-next') and not(ancestor::div[contains(@class, 'disabled')])]
        try:
            # 3. '활성화된' 다음 페이지 버튼을 찾습니다.
            next_button = driver.find_element(By.XPATH, "//a[contains(@class, 'page-next') and not(ancestor::div[contains(@class, 'disabled')])]")
    
            # 4. 버튼이 있으면 클릭하고 다음 페이지로 이동
            # 직접 클릭 대신 자바스크립트로 클릭 실행
            # 화면에 보이지 않아 해당 방법으로 대체함
            driver.execute_script("arguments[0].click();", next_button) # 자바스크립트 클릭 사용
            print("다음 페이지로 이동합니다.")
            time.sleep(5)  # 페이지 로드 대기
            continue  # 다음 페이지로 계속 진행
        
        # 5. '다음 페이지' 버튼이 비활성화 상태이면 루프 종료
        except NoSuchElementException: # 요소를 찾지 못했을 때 발생하는 예외 처리(클릭 버튼이 없다는 뜻)
            # 6. '활성화된' 버튼을 찾지 못하면 (비활성화 상태이거나 마지막 페이지)
            # NoSuchElementException 오류가 발생하며, 루프를 종료합니다.
            print("마지막 페이지에 도달했습니다. 크롤링을 종료합니다.\n")            
            break

    return s_list         


# 문자열 리스트를 Document 객체 리스트로 변환
def list_str_to_documents(text_list: list[dict]) -> list[Document]: # 매개변수 타입 힌트 추가 및 반환 타입 힌트 명시
    documents: list[Document] = []
    for text_dict in text_list:
        title = text_dict.get("title")
        body = text_dict.get("body")

        # Document 객체 생성
        doc = Document(
            page_content=body, # 문서내용
            metadata={ 
                "title": title, # 뉴스 제목 
                "document_type": "HTML" # 문서 유형
            }
        )
        documents.append(doc)
    return documents

# ---------------------------- txt 파일로 저장하는 함수 ---------------------------
# 리스트 딕셔너리가 아닌 txt 파일로 저장하는 함수 
# 제목과 본문 딕셔너리로 받음
def save_as_txt1(list_dict: list[dict]) -> None:
    html_text = list_dict
    date = datetime.datetime.now().strftime("%Y%m%d") # 20251116 형식
    date = "20251101"  # 테스트용 고정 날짜 (나중에 지우기)!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    # 파일명: news/bitkinds_news_20251116.txt ex) 2025년 11월 16일
    with open(f"txts/news/bitkinds_news_{date}.txt", "w", encoding="utf-8") as f: # 파일 쓰기 모드로 열기 f로 호출 
        for item in html_text:
            f.write(f"제목: {item['title']}\n")
            f.write(f"본문: {item['body']}\n")
            f.write("\n")

# metadata 포함 저장 함수(region_code, enactment_date)
# 41000은 경기도 지역 코드
def save_as_txt_with_metadata(list_dict: list[dict]) -> None: # region_code 차후에 수정
    news_list = list_dict # 리스트 딕셔너리(뉴스 제목과 본문)
    enactment_date = datetime.datetime.now().strftime("%Y%m%d") # 20251116 형식
    # enactment_date = "20251129"  # 테스트용 고정 날짜 (나중에 지우기)!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    region_code = "41000"  # 예시: 경기도 지역 코드 # 향후 수정 필요 ex -> "41000"
    # 파일명: news/bitkinds_news_20251116.txt ex) 2025년 11월 16일
    final_news_list = []
    for news in news_list:
        total_content = f"제목: {news['title']}\n내용: {news['body']}"
        structed_data = {
            "metadata": {
                "region_code": region_code,
                "enactment_date": enactment_date  
            },
            # content 안에 제목과 본문 포함
            "content": total_content
        }
        final_news_list.append(structed_data)

    with open(f"txts/news/bitkinds_news_{enactment_date}.txt", "w", encoding="utf-8") as f: # 파일 쓰기 모드로 열기 f로 호출
        for item in final_news_list:
            f.write(json.dumps(item, ensure_ascii=False))  # JSON 형식으로 저장
            f.write("\n")

# langchain용 Document 리스트로 반환하는 함수
def get_news_documents() -> list[Document]:
    driver = init_driver()
    wait = WebDriverWait(driver, 10)
    popup_handling(driver, wait)
    search_keyword(driver, wait)
    html_text = html_to_text_sel(driver, wait)
    documents = list_str_to_documents(html_text)
    driver.quit()
    return documents

def main():
    driver = init_driver()
    wait = WebDriverWait(driver, 10)
    popup_handling(driver, wait)
    search_keyword(driver, wait)
    html_text = html_to_text_sel(driver, wait)
    # save_as_txt1(html_text)
    save_as_txt_with_metadata(html_text)
    driver.quit()

# 스케줄러 설정
TARGET_TIME = "23:58"  # 매일 실행할 시간
schedule.every().day.at(TARGET_TIME).do(main) # do 안에 실행할 함수 넣기

if __name__ == "__main__":

    # 방법 1: langchain용 문서로 주기 (scheduler를 함수를 받은 모듈쪽에서 처리해줘야함)
    # document = get_news_documents()
    # for doc in document:
    #     print(f"Title: {doc.metadata['title']}")
    #     print(f"Content: {doc.page_content}") 
    #     print("-" * 50)

    main()
    # 방법 2: txt 파일로 주기 (독단적으로 이 스크립트가 항시 실행중이어야함)

    # 스케줄러 무한 루프 실행 (txt 파일로 저장하는 방식) 파일 이름 구조 : bitkinds_news_20251116.txt, bitkinds_news_20251117.txt 2025년 11월 16일, 17일
    # # 이 방법일시 독단적으로 이 스크립트가 항시 실행중이어야함 파일을 주기적으로 생성해줌
    # while True:
    #     schedule.run_pending()
    #     time.sleep(60)  # 1분마다 스케줄 확인    
