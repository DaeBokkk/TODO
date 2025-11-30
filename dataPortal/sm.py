# 단독/다가구 매매, 전월세 실거래가 데이터 수집 모듈

import requests
import xmltodict
import os
import dotenv
import datetime
from dataPortal import region
import json
# 초기화
dotenv.load_dotenv()
DATAGO_KEY = os.getenv("DATAGO_KEY")

# 단독/다가구 매매 거래 API 호출 함수
def sm_trade(lawd_code: str, deal_ym: str, n_rows: int = 9999) -> list[dict]:
    def _api_call(lawd_code: str, deal_ym: str, n_rows: int, page: int) -> dict:
        url = "https://apis.data.go.kr/1613000/RTMSDataSvcSHTrade/getRTMSDataSvcSHTrade"
        params = {
            "serviceKey": f"{DATAGO_KEY}",
            "LAWD_CD": f"{lawd_code}",
            "DEAL_YMD": f"{deal_ym}",
            "numOfRows": f"{n_rows}",
            "pageNo": f"{page}",
        }
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        return xmltodict.parse(resp.content)

    page: int = 1
    total_cnt: int = None
    result: list[dict] = []

    while True:
        parsed = _api_call(lawd_code=lawd_code, deal_ym=deal_ym, n_rows=n_rows, page=page)
        response: dict = parsed.get("response", {})
        header: dict = response.get("header", {})
        result_code = header.get("resultCode", "")
        if result_code == "000":
            body: dict = response.get("body", {})
            items: dict = body.get("items", {})
            if items:
                item: list = items.get("item", [])

                # 데이터가 1건일 때 딕셔너리로 반환 처리
                # item이 dict 타입이면 리스트로 변환
                # (예: {'a':1, 'b':2} -> [{'a':1, 'b':2}])
                # 이 부분이 없으면 1건일 때 오류 발생
                if isinstance(item, dict):  
                    item = [item]  # 리스트로 변환   

                result += item
                total_cnt = int(body.get("totalCount", 0))
                if len(result) >= total_cnt:
                    return result
                page += 1
            else:
                return result
        else:
            raise ValueError(f'[{result_code}] {header.get("resultMsg","")}')
        
# 단독/다가구 매매 실거래가 전체 반환 함수
def get_all_sm_trade_data(ym: str) -> list[dict]:

    all_sm_data = [] # 전체 단독/다가구 매매 데이터 리스트 세팅 

    print (f"=== {ym} 전국 단독/다가구 매매 데이터 수집 시작 ===")
    # 전국 '시/군/구' 법정동 코드 딕셔너리 조회
    sgg_code_dict = region.get_all_sgg_code_dict()
    total_regions = len(sgg_code_dict)

    for i, (region_name, lawd_cd) in enumerate(sgg_code_dict.items()):
        print(f" - [{i+1}/{total_regions}] {region_name} ({lawd_cd}) 데이터 수집 중...")
        sm_data = sm_trade(lawd_code=lawd_cd, deal_ym=ym)
        if sm_data:
            all_sm_data.extend(sm_data)
        else:
            print(f"   > {region_name} 지역 단독/다가구 매매 거래 데이터가 없습니다.")
    return all_sm_data

# 병합된 딕셔너리 리스트를 문자열로 반환 함수
def return_sm_trade_string(data: list[dict]) -> list[dict]:
    result_string: list[str] = []
    for record in data:
        record_str = (
            f"지역코드: {record.get('sggCd','')}\n"
            f"지역명: {record.get('umdNm','')}\n"
            f"주택유형(단독/다가구): {record.get('houseType','')}\n"
            f"지번: {record.get('jibun','')}\n"
            f"연면적(㎡): {record.get('totalFloorAr','')}\n"
            f"대지면적(㎡): {record.get('plottageAr','')}\n"
            f"계약년: {record.get('dealYear','')}\n"
            f"계약월: {record.get('dealMonth','')}\n"
            f"계약일: {record.get('dealDay','')}\n"
            f"거래금액(만원): {str(record.get('dealAmount','')).replace(',', '')}\n"
            f"건축년도: {record.get('buildYear','')}\n"
            f"해제여부: {record.get('cdealType','')}\n"
            f"해제사유발생일: {record.get('cdealDay','')}\n"
            f"거래유형(중개 및 직거래 여부): {record.get('dealingGbn','')}\n"
            f"중개사소재지(시군구 단위): {record.get('estateAgentSggNm','')}\n"
            f"거래주체정보_매도자(개인/법인/공공기관/기타): {record.get('slerGbn','')}\n"
            f"거래주체정보_매수자(개인/법인/공공기관/기타): {record.get('buyerGbn','')}\n"
        )

        # 문장화
        # ex 2025년 5월 12일에 서울특별시 강남구 역삼동에 위치한 단독주택(연면적 120.5㎡)이 5억 원에 매매 거래가 체결되었다. 건축년도는 2010년이다. 거래유형은 중개거래이며, 중개사소재지는 강남구이다. 매도자는 개인, 매수자도 개인이다.
        # record_str = (
        #     f"{str(record.get('dealYear',''))}년 {str(record.get('dealMonth',''))}월 {str(record.get('dealDay',''))}일에 "
        #     f"{record.get('umdNm','')}에 위치한 "
        #     f"{record.get('houseType','')}주택(연면적 {record.get('totalFloorAr','')}㎡)이 "
        #     f"{str(record.get('dealAmount','')).replace(',', '')}만원에 매매 거래가 체결되었다. "
        #     f"건축년도는 {record.get('buildYear','')}년이다. "
        #     f"거래유형은 {record.get('dealingGbn','')}이며, 중개사소재지는 {record.get('estateAgentSggNm','')}이다. "
        #     f"매도자는 {record.get('slerGbn','')}, 매수자도 {record.get('buyerGbn','')}이다."
        # )
        last_data = {
            "metadata": {
                "region_code": record.get('sggCd',''),
                "enactment_date": f"{str(record.get('dealYear',''))}{str(record.get('dealMonth','')).zfill(2)}{str(record.get('dealDay','')).zfill(2)}"
            },
            "content": record_str
        }
        result_string.append(last_data)
    return result_string

# txt 파일로 저장하는 함수 구현
def save_sm_trade_data_to_txt() -> None:
    
    now = datetime.datetime.now()
    year = now.year
    month = now.month
    day = now.day
    ym = f"{year}{month:02d}"
    total_df: list[dict] = get_all_sm_trade_data(ym)
    if not total_df:
        print(f"=== {ym} 기간에 조회된 단독/다가구 매매 실거래가 데이터가 전혀 없습니다. ===")
        return

    text_strings: list[dict] = return_sm_trade_string(total_df)

    # 중복 제거 로직 추가
    # 전날 파일 경로 
    yesterday = now - datetime.timedelta(days=1)
    yesterday_filedate = f"{yesterday.year}{yesterday.month:02d}{yesterday.day:02d}"    
    yesterday_filepath = f"txts/sm_real_estate/sm_data_{yesterday_filedate}.txt"
    
    previous_hashes = load_previous_hashes(yesterday_filepath)
    print(f"=== 이전 파일에서 {len(previous_hashes)}개의 중복 해시 로드 완료 ===")

    filtered_list: list[dict] = []
    for rent in text_strings:
        content = rent.get("content", "")
        content_hash = md5_hash(content)
        if content_hash not in previous_hashes:
            filtered_list.append(rent)
    print(f"=== 중복 제거 후 최종 저장할 데이터 건수: {len(filtered_list)}건 ===")
    ############# 중복 로직 끝 ######################

    filedate = f"{year}{month:02d}{day:02d}" # 파일명에 사용할 날짜 문자열 설정 -> YYYYMMDD
    filename = f"txts/sm_real_estate/sm_data_{filedate}.txt" # 파일명 설정 -> real_estate/sm_documents_YYYYMMDD.txt
    
    os.makedirs(os.path.dirname(filename), exist_ok=True) # 디렉토리 없으면 생성

    with open(filename, 'w', encoding='utf-8') as f:
        for text in filtered_list:
            f.write(json.dumps(text, ensure_ascii=False))
            f.write("\n")  # 각 문서 구분을 위한 빈 줄 추가
    print(f"텍스트 파일로 저장 완료: {filename}")

# 단독 다가구 전월세 거래 api 호출 함수
def sm_rent_trade(lawd_code: str, deal_ym: str, n_rows: int = 9999) -> list[dict]:
    def _api_call(lawd_code: str, deal_ym: str, n_rows: int, page: int) -> dict:
        url = "https://apis.data.go.kr/1613000/RTMSDataSvcSHRent/getRTMSDataSvcSHRent"
        params = {
            "serviceKey": f"{DATAGO_KEY}",
            "LAWD_CD": f"{lawd_code}",
            "DEAL_YMD": f"{deal_ym}",
            "numOfRows": f"{n_rows}",
            "pageNo": f"{page}",
        }
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        return xmltodict.parse(resp.content)

    page: int = 1
    total_cnt: int = None
    result: list[dict] = []

    while True:
        parsed = _api_call(lawd_code=lawd_code, deal_ym=deal_ym, n_rows=n_rows, page=page)
        response: dict = parsed.get("response", {})
        header: dict = response.get("header", {})
        result_code = header.get("resultCode", "")
        if result_code == "000":
            body: dict = response.get("body", {})
            items: dict = body.get("items", {})
            if items:
                item: list = items.get("item", [])

                # 데이터가 1건일 때 딕셔너리로 반환 처리
                # item이 dict 타입이면 리스트로 변환
                # (예: {'a':1, 'b':2} -> [{'a':1, 'b':2}])
                # 이 부분이 없으면 1건일 때 오류 발생
                if isinstance(item, dict):  
                    item = [item]  # 리스트로 변환   

                result += item
                total_cnt = int(body.get("totalCount", 0))
                if len(result) >= total_cnt:
                    return result
                page += 1
            else:
                return result
        else:
            raise ValueError(f'[{result_code}] {header.get("resultMsg","")}')
        
# 단독/다가구 전월세 실거래가 전체 반환 함수
def get_all_sm_rent_data(ym: str) -> list[dict]:

    all_sm_rent_data = [] # 전체 단독/다가구 전월세 데이터 리스트 세팅 

    print (f"=== {ym} 전국 단독/다가구 전월세 데이터 수집 시작 ===")
    # 전국 '시/군/구' 법정동 코드 딕셔너리 조회
    sgg_code_dict = region.get_all_sgg_code_dict()
    total_regions = len(sgg_code_dict)

    for i, (region_name, lawd_cd) in enumerate(sgg_code_dict.items()):
        print(f" - [{i+1}/{total_regions}] {region_name} ({lawd_cd}) 데이터 수집 중...")
        sm_rent_data = sm_rent_trade(lawd_code=lawd_cd, deal_ym=ym)
        if sm_rent_data:
            all_sm_rent_data.extend(sm_rent_data)
        else:
            print(f"   > {region_name} 지역 단독/다가구 전월세 거래 데이터가 없습니다.")
    return all_sm_rent_data

# 병합된 딕셔너리 리스트를 문자열로 반환 함수
def return_sm_rent_string(data: list[dict]) -> list[dict]:
    result_string: list[str] = []
    for record in data:
        record_str = (
            f"지역코드: {record.get('sggCd','')}\n"
            f"지역명: {record.get('umdNm','')}\n"
            f"주택유형(단독/다가구): {record.get('houseType','')}\n"
            f"연면적(㎡): {record.get('totalFloorAr','')}\n"
            f"계약년: {record.get('dealYear','')}\n"
            f"계약월: {record.get('dealMonth','')}\n"
            f"계약일: {record.get('dealDay','')}\n"
            f"보증금액(만원): {str(record.get('deposit','')).replace(',', '')}\n"
            f"월세금액(만원): {str(record.get('monthlyRent','')).replace(',', '')}\n"
            f"건축년도: {record.get('buildYear','')}\n"
            f"계약기간: {record.get('contractTerm','')}\n"
            f"계약구분: {record.get('contractType','')}\n"
            f"갱신요구권사용: {record.get('useRRRight','')}\n"
            f"종전계약보증금(만원): {str(record.get('preDeposit','')).replace(',', '')}\n"
            f"종전계약월세(만원): {str(record.get('preMonthlyRent','')).replace(',', '')}\n"
        )

        # 문장화
        # ex 2025년 5월 12일에 서울특별시 강남구 역삼동에 위치한 단독주택(연면적 120.5㎡)이 보증금 5000만원, 월세 50만원에 전월세 계약이 체결되었다. 건축년도는 2010년이며, 계약기간은 2년이다. 계약구분은 신규계약이며, 갱신요구권은 사용되지 않았다. 종전계약보증금은 4500만원, 종전계약월세는 45만원이었다.
        # record_str = (
        #     f"{str(record.get('dealYear',''))}년 {str(record.get('dealMonth',''))}월 {str(record.get('dealDay',''))}일에 "
        #     f"{record.get('umdNm','')}에 위치한 "
        #     f"{record.get('houseType','')}(연면적 {record.get('totalFloorAr','')}㎡)이 "
        #     f"보증금 {str(record.get('deposit','')).replace(',', '')}만원, "
        #     f"월세 {str(record.get('monthlyRent','')).replace(',', '')}만원에 전월세 계약이 체결되었다. "
        #     f"건축년도는 {record.get('buildYear','')}년이며, "
        #     f"계약기간은 {record.get('contractTerm','')}이다. "
        #     f"계약구분은 {record.get('contractType','')}이며, "
        #     f"갱신요구권은 {record.get('useRRRight','')}하였다. "
        #     f"종전계약보증금은 {str(record.get('preDeposit','')).replace(',', '')}만원, "
        #     f"종전계약월세는 {str(record.get('preMonthlyRent','')).replace(',', '')}만원이었다."
        # )

        # 메타데이터 추가
        last_data = {
            "metadata": {
                "region_code": record.get('sggCd',''),
                "enactment_date": f"{str(record.get('dealYear',''))}{str(record.get('dealMonth','')).zfill(2)}{str(record.get('dealDay','')).zfill(2)}"
            },
            "content": record_str
        }

        result_string.append(last_data)
    return result_string

# txt 파일로 저장하는 함수 구현
def save_sm_rent_data_to_txt() -> None:
    
    now = datetime.datetime.now()
    year = now.year
    month = now.month
    day = now.day
    ym = f"{year}{month:02d}"
    total_df: list[dict] = get_all_sm_rent_data(ym)
    if not total_df:
        print(f"=== {ym} 기간에 조회된 단독/다가구 전월세 실거래가 데이터가 전혀 없습니다. ===")
        return
    
    text_strings: list[dict] = return_sm_rent_string(total_df)

    # 중복 제거 로직 추가
    # 전날 파일 경로
    yesterday = now - datetime.timedelta(days=1)
    yesterday_filedate = f"{yesterday.year}{yesterday.month:02d}{yesterday.day:02d}"    
    yesterday_filepath = f"txts/sm_real_estate/sm_rent_data_{yesterday_filedate}.txt"
    
    previous_hashes = load_previous_hashes(yesterday_filepath)
    print(f"=== 이전 파일에서 {len(previous_hashes)}개의 중복 해시 로드 완료 ===")  

    filtered_list: list[dict] = []
    
    for rent in text_strings:
        content = rent.get("content", "")
        content_hash = md5_hash(content)
        if content_hash not in previous_hashes:
            filtered_list.append(rent)
    print(f"=== 중복 제거 후 최종 저장할 데이터 건수: {len(filtered_list)}건 ===")
    ############# 중복 로직 끝 ######################

    filedate = f"{year}{month:02d}{day:02d}" # 파일명에 사용할 날짜 문자열 설정 -> YYYYMMDD
    filename = f"txts/sm_real_estate/sm_rent_data_{filedate}.txt" # 파일명 설정 -> real_estate/sm_rent_documents_YYYYMMDD.txt

    os.makedirs(os.path.dirname(filename), exist_ok=True) # 디렉토리 없으면 생성

    with open(filename, 'w', encoding='utf-8') as f:
        for text in filtered_list:
            f.write(json.dumps(text, ensure_ascii=False))
            f.write("\n")  # 각 문서 구분을 위한 빈 줄 추가
    print(f"텍스트 파일로 저장 완료: {filename}")


# 문자열의 MD5 해시값 계산 함수
def md5_hash(text: str) -> str:
    import hashlib
    return hashlib.md5(text.encode('utf-8')).hexdigest()

# 전날 txt에서 content만 읽어서 set으로 반환하는 함수
def load_previous_hashes(filepath: str) -> set:
    """전날 txt에서 content 해시만 읽어서 set으로 반환"""
    if not os.path.exists(filepath):
        return set()

    hashes = set()
    with open(filepath, 'r', encoding='utf-8') as f:
        blocks = f.read().strip().split("\n")

        for block in blocks:
            block = block.strip()
            if not block:
                continue

            try:
                doc = json.loads(block)
                content = doc.get("content", "")
                content_hash = md5_hash(content)
                hashes.add(content_hash)
            except:
                continue

    return hashes

# 스케줄 설정
import schedule
import time
schedule.every(1).days.do(save_sm_trade_data_to_txt)
schedule.every(1).days.do(save_sm_rent_data_to_txt)

# --- 스크립트 실행 (Main Pipeline) ---
if __name__ == "__main__":
    # # 단독/다가구 매매 실거래가 데이터 txt 파일로 저장
    save_sm_trade_data_to_txt()
    
    # # 단독/다가구 전월세 실거래가 데이터 txt 파일로 저장
    save_sm_rent_data_to_txt()

    while True:
        schedule.run_pending()
        # 30분 대기
        time.sleep(1)