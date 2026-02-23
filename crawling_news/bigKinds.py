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

    # options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")

    driver = webdriver.Chrome(options=options)
    url = "https://www.bigkinds.or.kr/"
    driver.get(url)
    return driver


# 메인 페이지 팝업창 2개 처리 함수
# def popup_handling(driver: webdriver.Chrome, wait: WebDriverWait) -> None:
#     # try:
#     #     popup1_close_xpath = '//*[@id="popup-dialog-127"]/div/div[2]/div/div[2]/button'
#     #     close_button1 = wait.until(EC.element_to_be_clickable((By.XPATH, popup1_close_xpath)))
#     #     close_button1.click()
#     #     print("첫 번째 팝업을 닫았습니다.")
#     # except Exception as e:
#     #     print(f"첫 번째 팝업 닫기 실패: {e}")
#     # try:
#     #     popup2_close_xpath = '//*[@id="popup-dialog-128"]/div/div[2]/div/div[2]/button'
#     #     close_button2 = wait.until(EC.element_to_be_clickable((By.XPATH, popup2_close_xpath)))
#     #     close_button2.click()
#     #     print("두 번째 팝업을 닫았습니다.")
#     # except Exception as e:
#     #     print(f"두 번째 팝업 닫기 실패: {e}")
    
#     # try:
#     #     # //*[@id="popup-dialog-128"]/div/div[2]/div/div[2]/button
#     #     popup3_close_xpath = '//*[@id="popup-dialog-128"]/div/div[2]/div/div[2]/button'
#     #     close_button3 = wait.until(EC.element_to_be_clickable((By.XPATH, popup3_close_xpath)))
#     #     close_button3.click()
#     #     print("팝업을 닫았습니다.")
#     # except Exception as e:
#     #     print(f"팝업 닫기 실패: {e}")
    
#     try:
#         # //*[@id="popup-dialog-127"]/div/div[2]/div/div[2]/button
#         popup4_close_xpath = '//*[@id="popup-dialog-135"]/div/div[2]/div/div[2]/button'
#         close_button4 = wait.until(EC.element_to_be_clickable((By.XPATH, popup4_close_xpath)))
#         close_button4.click()
#         print("팝업을 닫았습니다.")
#     except Exception as e:
#         print(f"팝업 닫기 실패: {e}")

def popup_handling(driver: webdriver.Chrome, wait: WebDriverWait) -> None:
    while True:
        try:
            btn = wait.until(EC.element_to_be_clickable(
                (By.XPATH, "//*[starts-with(@id, 'popup-dialog-')]/div/div[2]/div/div[2]/button")
            ))
            btn.click()
            print("팝업을 닫았습니다.")
            time.sleep(0.1)  # 다음 팝업 뜨는 시간 약간 기다리기
        except:
            break

    print("모든 팝업 처리가 끝났습니다.")


# 검색 키워드 입력 및 필터 설정
def search_keyword(driver: webdriver.Chrome, wait: WebDriverWait) -> None:
    # 오늘날짜일형식 20251116
    # 2025-11-16 형식으로 변환
    # s_today = "2026-02-14" 
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    # today = "2026-02-16"  # 테스트용 고정 날짜 (나중에 지우기)!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
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

    action = webdriver.ActionChains(driver)

    action.click(search_begin_date).pause(0.02).click(search_begin_date).pause(0.02).click(search_begin_date).perform()
    search_begin_date.send_keys(Keys.DELETE)
    search_begin_date.send_keys(today) # 시작 날짜 입력
    search_begin_date.send_keys(Keys.ENTER)
    print(f"오늘날짜 {today} 입력했습니다.")

    action.click(search_end_date).pause(0.02).click(search_end_date).pause(0.02).click(search_end_date).perform()
    search_end_date.send_keys(Keys.DELETE)
    search_end_date.send_keys(today)
    search_end_date.send_keys(Keys.ENTER)
    # 뉴스 검색 기간 설정 끝

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
    s_list = [] 
    visited_ids = set()  # [핵심] 뉴스 고유 ID 저장소 (중복 방지용)
    
    # ---------------------------------------------------------------------------
    # 1. 분석 제외 버튼 처리 (기존 동일)
    try:
        exclude_xpath = '//*[@id="filterTab05"]/li[1]/span/label'
        exclude_button = wait.until(EC.presence_of_element_located((By.XPATH, exclude_xpath)))
        driver.execute_script("arguments[0].click();", exclude_button)
        print("분석 제외 버튼을 클릭했습니다 (JS 실행).")
        time.sleep(0.5) 
    except Exception as e:
        print(f"분석 제외 버튼 클릭 실패(무시): {e}")
    # ---------------------------------------------------------------------------

    # 2. 뉴스 리스트 대기
    # 리스트의 각 아이템(컨테이너)을 잡는 XPath
    # 이 div에 'data-id' 속성이 들어있습니다.
    base_item_xpath = '//*[@id="news-results"]/div' 
    
    try:
        print("뉴스 검색 결과를 기다리고 있습니다...")
        wait.until(EC.presence_of_all_elements_located((By.XPATH, base_item_xpath)))
    except TimeoutException:
        print(">>> 검색된 뉴스가 없습니다. (빈 리스트 반환)")
        return []

    while True:
        # 1. 현재 페이지의 모든 뉴스 아이템 요소 가져오기
        try:
            # DOM이 변경될 수 있으므로 매 페이지마다 새로 요소를 찾습니다.
            news_items = wait.until(EC.presence_of_all_elements_located((By.XPATH, base_item_xpath)))
            count = len(news_items)
            print(f"현재 페이지의 뉴스 아이템 수: {count}")
        except:
            print("뉴스 아이템을 찾을 수 없습니다. 다음 페이지 또는 종료.")
            break

        # 2. 인덱스로 순회
        for i in range(count):
            try:
                # i번째 요소 다시 찾기 
                # XPath는 1부터 시작하므로 i+1
                current_item_xpath = f'//*[@id="news-results"]/div[{i+1}]'
                item_container = driver.find_element(By.XPATH, current_item_xpath)
                
                # data-id 추출
                # 빅카인즈는 <div class="news-item" data-id="01100101.2025..."> 형태로 뉴스 고유 ID를 제공
                news_id = item_container.get_attribute("data-id")

                if not news_id:
                    # 만약 data-id가 없으면 안전하게 넘어가거나 로그 남기기
                    print(f"{i+1}번째 뉴스: ID를 찾을 수 없어 건너뜁니다.")
                    continue

                # 중복 검사 (이미 수집한 ID면 클릭 안 함)
                if news_id in visited_ids:
                    print(f"중복 : {news_id}")
                    continue
                
                # 중복이 아니면 ID 저장
                visited_ids.add(news_id)

                # -------------------------------------------------------
                # 뉴스 클릭 처리
                # 클릭 대상: 제목/본문이 있는 내부 div
                click_target_xpath = f'{current_item_xpath}/div/div[2]/a/div'
                click_element = wait.until(EC.element_to_be_clickable((By.XPATH, click_target_xpath)))
                driver.execute_script("arguments[0].click();", click_element)
                # print(f"클릭: {news_id}")

                # -------------------------------------------------------
                # 상세 모달 내용 수집
                
                # 모달이 뜨고 데이터가 로딩될 때까지 '제목'을 기준으로 대기
                title_xpath = '//*[@id="news-detail-modal"]/div/div/div/div[1]/div/div[1]/h1'
                
                # 모달 제목이 비어있지 않을 때까지 대기 (로딩 지연 방지)
                article_title = wait.until(lambda d: d.find_element(By.XPATH, title_xpath) 
                                           if d.find_element(By.XPATH, title_xpath).text.strip() != "" 
                                           else False)
                
                article_title_text = article_title.text.strip()
                
                # 본문 추출
                body_xpath = '//*[@id="news-detail-modal"]/div/div/div/div[1]/div/div[2]/div[2]' 
                try:
                    article_body = wait.until(lambda d: d.find_element(By.XPATH, body_xpath))
                except:
                    # 이미지가 없는 경우 등 대체 XPath
                    body_xpath_alt = '//*[@id="news-detail-modal"]/div/div/div/div[1]/div/div[2]/div'
                    article_body = wait.until(lambda d: d.find_element(By.XPATH, body_xpath_alt))

                raw_body_text = article_body.text
                
                # [데이터 전처리] 불필요한 공백 및 문구 제거 (RAG 품질 향상)
                cleaned_body_text = re.sub(r'\s+', ' ', raw_body_text).strip() 
                
                # -------------------------------------------------------
                # 데이터 수집 성공 -> 리스트에 추가
                print(f"수집 완료 [{i+1}/{count}]: {article_title_text}")
                
                s_list.append({
                    "title": article_title_text,
                    "body": cleaned_body_text
                })

                # -------------------------------------------------------
                # 모달 닫기
                close_btn_xpath = '//*[@id="news-detail-modal"]/div/div/div/button'
                close_button = wait.until(EC.presence_of_element_located((By.XPATH, close_btn_xpath)))
                driver.execute_script("arguments[0].click();", close_button)
                
                # 모달 닫힘 대기 (안정성 확보)
                time.sleep(0.3)

            except Exception as e:
                print(f"에러 발생 ({i+1}번째 뉴스): {e}")
                # 에러 발생 시 모달이 열려있다면 닫아줘야 다음 루프가 정상 작동함
                try:
                    close_btn = driver.find_element(By.XPATH, '//*[@id="news-detail-modal"]/div/div/div/button')
                    driver.execute_script("arguments[0].click();", close_btn)
                except:
                    pass
                continue

        # --- 다음 페이지 이동 ---
        try:
            # 'page-next' 클래스가 있고, 부모 div에 'disabled' 클래스가 없는 버튼 찾기
            next_button_xpath = "//a[contains(@class, 'page-next') and not(ancestor::div[contains(@class, 'disabled')])]"
            next_button = driver.find_element(By.XPATH, next_button_xpath)
            
            driver.execute_script("arguments[0].click();", next_button)
            print(">>> 다음 페이지로 이동합니다.")
            time.sleep(1.5) # 페이지 로드 대기
            
        except NoSuchElementException:
            print("마지막 페이지에 도달했습니다. 크롤링을 종료합니다.\n")            
            break
        except Exception as e:
            print(f"페이지 이동 중 에러: {e}")
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
    # date = "20251101"  # 테스트용 고정 날짜 (나중에 지우기)!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
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
    # enactment_date = "20260131"  # 테스트용 고정 날짜 (나중에 지우기)!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    region_code = "41000"  # 예시: 경기도 지역 코드 # 향후 수정 필요 ex -> "41000" # 경기도 
    # 파일명: news/bitkinds_news_20251116.txt ex) 2025년 11월 16일
    final_news_list = []
    for news in news_list:
        total_content = f"제목: {news['title']}\n내용: {news['body']}"
        structed_data = {
            "metadata": {
                "region_code": region_code, # 지역 코드 
                "enactment_date": enactment_date # 오늘 날짜 
            },
            # content 안에 제목과 본문 포함
            "content": total_content
        }
        final_news_list.append(structed_data)
        
    # final_news_list가 null이면 저장하지 않음
    if len(final_news_list) == 0:
        print("=== 저장할 뉴스 데이터가 없습니다. 파일 저장을 수행하지 않습니다. ===")
        return

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