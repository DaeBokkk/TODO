# https://www.law.go.kr/lbook/lbListR.do?menuId=13&subMenuId=67&tabMenuId=293
# selenium을 이용한 법령 크롤링 모듈
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
import time
from bs4 import BeautifulSoup
import json 
import re
from datetime import datetime
# "부동산, 공동주택 분양 관련 모음집(AI RAG 버전)" 검색 키워드 

def init_driver() -> webdriver.Chrome:
    url = "https://www.law.go.kr/lbook/lbListR.do?menuId=13&subMenuId=67&tabMenuId=293"
    options = webdriver.ChromeOptions()
    # options.add_argument("--headless=new")  # 헤드리스 모드 설정
    # options.add_argument("--no-sandbox")
    # options.add_argument("--disable-dev-shm-usage")
    service = Service()  # ChromeDriver 경로를 지정하지 않으면 기본 경로 사용
    driver = webdriver.Chrome(service=service, options=options)
    driver.get(url)
    return driver

# 날짜 추출 함수
def extract_enactment_date(text: str) -> str:
    """
    텍스트에서 '시행 20XX. X. X.' 패턴을 찾아 'YYYYMMDD' 형식으로 반환
    실패 시 오늘 날짜 반환
    """
    try:
        # 정규식: '시행' + 공백 + (숫자4개. 숫자1~2개. 숫자1~2개.)
        match = re.search(r'시행\s+(\d{4}\.\s*\d{1,2}\.\s*\d{1,2}\.)', text)
        if match:
            raw_date = match.group(1) # "2024. 5. 17."
            # 날짜 파싱 (공백 유연하게 처리)
            dt = datetime.strptime(raw_date.strip(), "%Y. %m. %d.")
            return dt.strftime("%Y%m%d") # "20240517"
    except Exception as e:
        print(f"날짜 추출 중 오류: {e}")
    
    # 실패 시 기본값 (오늘 날짜)
    return datetime.now().strftime("%Y%m%d")

# 검색어 입력 및 검색 실행
def search_law_keyword(driver: webdriver.Chrome, wait: WebDriverWait) -> list[dict]:
    # //*[@id="searchKeyword"]
    search_box = wait.until(EC.presence_of_element_located((By.ID, "searchKeyword")))
    search_box.clear()
    search_box.send_keys("부동산, 공동주택 분양 관련 모음집")
    time.sleep(1)  # 입력 대기

    # 검색 버튼 클릭
    # //*[@id="container2"]/div[3]/span/a
    search_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="container2"]/div[3]/span/a')))
    search_button.click()
    time.sleep(2)  # 검색 결과 로딩 대기

    # 검색 결과 첫 번째 법령 클릭
    # //*[@id="container2"]/table/tbody/tr/td[2]/a
    result = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="container2"]/table/tbody/tr/td[2]/a')))
    result.click()
    time.sleep(1)

    # 팝업 창 전환
    main_window = driver.current_window_handle # 메인 윈도우 핸들 저장
    all_windows = driver.window_handles # 모든 윈도우 핸들 저장

    # 팝업 창으로 전환
    for w in all_windows: 
        if w != main_window: # 설명 : 메인 윈도우가 아니면
            popup_window = w # 설명 : 팝업 윈도우로 설정

    driver.switch_to.window(popup_window)
    print("팝업 창으로 전환 완료")

    # 법령 본문 xpath 전체 추출 
    # //*[@id="contents"]/div[2]/div/ul/li[1]/a
    # //*[@id="contents"]/div[2]/div/ul/li[2]/a
    # //*[@id="contents"]/div[2]/div/ul/li[3]/a
    # ...
    # //*[@id="contents"]/div[2]/div/ul/li[110]/a
    # law_links에는 법령 링크들이 모두 들어있음
    law_links = wait.until(EC.presence_of_all_elements_located((By.XPATH, '//*[@id="contents"]/div[2]/div/ul/li/a')))
    
    total_laws: list[str] = [] # 법령 본문 전체를 담을 리스트
    
    # 각 법령 링크를 순회하며 본문 크롤링
    # 5개만 테스트
    # law_links = law_links[:5]  # 테스트용으로 5개만 크롤링

    for i, link in enumerate(law_links, start=1):
        print(f"법령 {i}번째 링크 텍스트: {link.text}")
        link.click()

        # 창이 새로 뜨는 구조이므로 새 창으로 전환
        all_windows = driver.window_handles
        for w in all_windows:
            if w != main_window and w != popup_window:
                law_window = w
        driver.switch_to.window(law_window)
        time.sleep(1)  # 법령 본문 페이지 로딩 대기
        

        if driver.page_source :
            try: 
                target_xpath = '//*[@id="conScroll"]' # 법령 본문 영역 xpath
                # d는 driver
                # 법령 본문 로딩 대기 20글자 이상 채워질때까지 대기 자바로 본문을 로딩하기 때문에 이와 같이 수정
                wait.until(lambda d = driver: len(d.find_element(By.XPATH, target_xpath).text.strip()) > 20) 
            except:
                print(f"법령 {i}번째 본문 로딩 실패")
                continue
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            title_elem = soup.select_one('#conTop > h2') # 법령 제목
            info_elem = soup.select_one('#conTop > div > span') # 법령 정보
            # div 클래스 이름 pgroup
            # #conScroll > div.cont_subtit
            subtitle_elem = soup.select_one('#conScroll > div.cont_subtit') # 소제목들
            subtitle_elem_texts = subtitle_elem.get_text(strip=True) if subtitle_elem else '' # '국토교통부(주택정책과), 044-201-4089'

            content_elem = soup.select_one('#conScroll') # 법령 본문 내용 전체 

            sub_elem = soup.select_one('#arDivArea') # 부가 정보 영역
            sub_elem_text = sub_elem.get_text(strip=True) if sub_elem else '' # 부칙 텍스트 미리 추출
            # 부      칙 -> 공백 제거
            sub_elem_text = sub_elem_text.replace('부      칙', '\n부칙')
            # 내용 중 불필요한 마지막 <ul> 요소 제거 (마지막 별표 부분) 생략함
            if content_elem:
                ul_list = content_elem.find_all('ul', class_='pconfile pb30 pcf_sec01')
                for ul in ul_list:
                    ul.decompose()  # 해당 요소 제거
                # 부칙 제거 (예: <div id="arDivArea">)
            if content_elem:
                ar_div = content_elem.find('div', id='arDivArea')
                if ar_div:
                    ar_div.decompose()  # 부칙 영역 제거 (decompose() 메서드는 재할당하지 않아도 요소를 제거함)
            if content_elem:
                # #conScroll > div.cont_subtit 요소 제거 ex: 국토교통부(주택정책과), 044-201-4089
                cont_subtit_divs = content_elem.find_all('div', class_='cont_subtit')
                for div in cont_subtit_divs:
                    div.decompose()  # 해당 요소 제거

            # 나머지 텍스트 추출 및 병합
            title_elem_text = title_elem.get_text(strip=True) if title_elem else '' # ex : '부동산 거래신고 등에 관한 법률'
            info_elem_text = info_elem.get_text(strip=True) if info_elem else '' # ex : '제정 2020.12.8 법률 제17513호'
            content_elem_text = content_elem.get_text(strip=False) if content_elem else '' # 법령 본문 전체 텍스트
            # 본문 각각의 줄바꿈 \n 하나만 살리기 
            # 함수 설명 : 여러줄로 된 텍스트에서 각 줄의 앞뒤 공백을 제거하고, 빈 줄(공백만 있는 줄)은 제거한 후, 나머지 줄들을 하나의 문자열로 합칩니다.
            content_elem_text = '\n'.join([line.strip() for line in content_elem_text.splitlines() if line.strip() != ''])
            
            # 전체 법령 본문 텍스트 병합
            full_law_text = f"{title_elem_text}\n{info_elem_text}\n{subtitle_elem_texts}\n{content_elem_text}{sub_elem_text}"
            # 날짜 추출
            enactment_date = extract_enactment_date(info_elem_text)
            total_laws_with_meta = {
                "metadata": {
                    "region_code": "41000",  # 예시: 경기도 지역 코드 # 향후 수정 필요 ex -> "41000"
                    "enactment_date": enactment_date  # 추출된 법령 제정일
                },
                "content": full_law_text
            }
            total_laws.append(total_laws_with_meta)
            # 법령 본문 리스트에 추가
            print(f"법령 {i}번째 본문 내용 추출 (BeautifulSoup 사용)")
            # /////////////////////////////////////////////////////
            # 법령 본문 페이지 닫기
            driver.close()
            time.sleep(1)
            # 팝업 창으로 다시 전환
            driver.switch_to.window(popup_window)
            time.sleep(1)

    print(f"총 {len(total_laws)}개의 법령 본문 크롤링 완료.")
    return total_laws

# 문자열 리스트를 텍스트 파일로 저장
def save_as_txt(law_texts: list[dict]) -> None:
    filename = "txts/laws/law_texts.txt"
    with open(filename, 'w', encoding="utf-8") as f:
        for i, text in enumerate(law_texts, start=1):
            f.write(f"=== 법령 {i}번째 본문 시작 ===\n")
            f.write(text)
            f.write(f"\n=== 법령 {i}번째 본문 끝 ===\n\n")
    print(f"법령 본문이 '{filename}' 파일로 저장되었습니다.")

# 메타데이터 포함 전체 파이프라인 함수(region_code, enactment_date 포함)
def save_as_txt_with_metadata(law_texts: list[dict]) -> None:
    filename = "txts/laws/law_texts_with_metadata.txt"
    # json 형식으로 저장
    with open(filename, 'w', encoding="utf-8") as f:
        for law in (law_texts):
            f.write(json.dumps(law, ensure_ascii=False))
            f.write("\n")  # 각 법령 사이에 줄바꿈 추가

def main():
    driver = init_driver()
    wait = WebDriverWait(driver, 10)

    laws = search_law_keyword(driver, wait)
    # save_as_txt(laws)
    save_as_txt_with_metadata(laws)

    driver.quit()

if __name__ == "__main__":
    main()

