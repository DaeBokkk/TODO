# 연립다세대 매매/전월세 데이터 수집 모듈 

import requests
import xmltodict
from dataPortal import region
import os 
import dotenv
import datetime
import json
# 초기화
dotenv.load_dotenv()
DATAGO_KEY = os.getenv("DATAGO_KEY")

# 연립다세대 매매 실거래가 api 호출 함수
def rh_trade(lawd_code: str, deal_ym: str, n_rows: int = 9999) -> list[dict]:
    def _api_call(lawd_code: str, deal_ym: str, n_rows: int, page: int) -> dict:
        url = "https://apis.data.go.kr/1613000/RTMSDataSvcRHTrade/getRTMSDataSvcRHTrade"
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
    

# 연립다세대 매매 실거래가 전체 반환 함수
def get_all_rh_trade_data(ym: str) -> list[dict]:

    all_rh_data = []
    # 전국 '시/군/구' 법정동 코드 딕셔너리 조회
    region_dict = region.get_all_sgg_code_dict()

    for region_name, lawd_code in region_dict.items():
        print(f"=== {region_name} ({lawd_code}) 지역의 연립다세대 매매 실거래가 데이터 수집 중... ===")
        try:
            region_data = rh_trade(lawd_code=lawd_code, deal_ym=ym)
            all_rh_data.extend(region_data)
            print(f"  -> {region_name} 지역에서 {len(region_data)}건의 데이터 수집 완료.")
        except Exception as e:
            print(f"  !!! {region_name} 지역 데이터 수집 중 오류 발생: {e}")
    
    print(f"=== 전체 연립다세대 매매 실거래가 데이터 수집 완료. 총 {len(all_rh_data)}건 수집됨. ===")
    return all_rh_data

# 병합된 딕셔너리 리스트를 문자열로 반환 함수
def return_rh_trade_string(data: list[dict]) -> list[dict]:
    result_string: list[str] = []
    for record in data:
        record_str = (
            f"지역코드: {str(record.get('sggCd',''))}\n"
            f"법정동명: {str(record.get('umdNm',''))}\n"
            f"연립다세대명: {str(record.get('mhouseNm', ''))}\n"
            f"지번: {str(record.get('jibun', ''))}\n"
            f"건축년도: {str(record.get('buildYear', ''))}\n"
            f"전용면적(㎡): {str(record.get('excluUseAr',''))}\n"
            f"대지권면적(㎡): {str(record.get('landAr',''))}\n"
            f"계약년도: {str(record.get('dealYear',''))}\n"
            f"계약월: {str(record.get('dealMonth',''))}\n"
            f"계약일: {str(record.get('dealDay',''))}\n"
            f"거래금액(만원): {str(record.get('dealAmount','')).replace(',', '')}\n"
            f"층: {str(record.get('floor', ''))}\n"
            f"해제여부: {str(record.get('cdealType',''))}\n"
            f"해제사유발생일: {str(record.get('cdealDay',''))}\n"
            f"거래유형(중개 및 직거래 여부): {str(record.get('dealingGbn',''))}\n"
            f"중개사소재지(시군구 단위): {str(record.get('estateAgentSggNm',''))}\n"
            f"등기일자: {str(record.get('rgstDate',''))}\n"
            f"거래주체정보_매도자(개인/법인/공공기관/기타): {str(record.get('slerGbn',''))}\n"
            f"거래주체정보_매수자(개인/법인/공공기관/기타): {str(record.get('buyerGbn',''))}\n"
        )

        last_data = {
            "metadata": {
                "REGION_CODE": record.get('sggCd',''),
                "ENACTMENT_DATA": f"{record.get('dealYear','')}{record.get('dealMonth','')}{record.get('dealDay','')}"
            },
            "content": record_str
        }

        result_string.append(last_data)

    return result_string

# txt 파일로 저장하는 함수
def save_rh_trade_data_to_txt() -> None:
    # 날짜 설정
    now = datetime.datetime.now()
    year = now.year
    month = now.month
    day = now.day
    ym = f"{year}{month:02d}"
    filedate = f"{year}{month:02d}{day:02d}" # 파일명에 사용할 날짜 문자열 설정 -> YYYYMMDD
    # 데이터 수집
    all_rh_data = get_all_rh_trade_data(ym=ym)

    # 문자열 리스트로 변환
    rh_strings = return_rh_trade_string(all_rh_data)

    # 중복 제거 로직 시작
    yesterday = now - datetime.timedelta(days=1)
    yesterday_filedate = f"{yesterday.year}{yesterday.month:02d}{yesterday.day:02d}"    
    yesterday_filepath = f"txts/rh_real_estate/rh_data_{yesterday_filedate}.txt"    
    previous_hashes = load_previous_hashes(yesterday_filepath)
    print(f"=== 이전 파일에서 {len(previous_hashes)}개의 중복 해시 로드 완료 ===")

    filtered_list: list[dict] = []

    for rent in rh_strings:
        content = rent.get("content", "")
        content_hash = md5_hash(content)
        if content_hash not in previous_hashes:
            filtered_list.append(rent)
    print(f"=== 중복 제거 후 최종 저장할 데이터 건수: {len(filtered_list)}건 ===")
    # 중복 제거 로직 끝

    # 파일명 설정
    filename = f"txts/rh_real_estate/rh_data_{filedate}.txt"

    # 텍스트 파일로 저장
    with open(filename, "w", encoding="utf-8") as f:
        for record in filtered_list:
            f.write(json.dumps(record, ensure_ascii=False))
            f.write("\n")  # 각 기록 사이에 줄바꿈 추가

    print(f"연립다세대 매매 실거래가 데이터가 '{filename}' 파일로 저장되었습니다.")


# 연립다세대 전월세 실거래가 api 호출 함수
def rh_rent_trade(lawd_code: str, deal_ym: str, n_rows: int = 9999) -> list[dict]:
    def _api_call(lawd_code: str, deal_ym: str, n_rows: int, page: int) -> dict:
        url = "https://apis.data.go.kr/1613000/RTMSDataSvcRHRent/getRTMSDataSvcRHRent"
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
        
# 연립다세대 전월세 실거래가 전체 반환 함수
def get_all_rh_rent_data(ym: str) -> list[dict]:

    all_rh_rent_data = [] # 전체 연립다세대 전월세 데이터 리스트 세팅 

    print (f"=== {ym} 전국 연립다세대 전월세 데이터 수집 시작 ===")
    
    region_dict = region.get_all_sgg_code_dict()

    for region_name, lawd_code in region_dict.items():
        print(f"=== {region_name} ({lawd_code}) 지역의 연립다세대 전월세 실거래가 데이터 수집 중... ===")
        try:
            region_data = rh_rent_trade(lawd_code=lawd_code, deal_ym=ym)
            all_rh_rent_data.extend(region_data)
            print(f"  -> {region_name} 지역에서 {len(region_data)}건의 데이터 수집 완료.")
        except Exception as e:
            print(f"  !!! {region_name} 지역 데이터 수집 중 오류 발생: {e}")
    
    print(f"=== 전체 연립다세대 전월세 실거래가 데이터 수집 완료. 총 {len(all_rh_rent_data)}건 수집됨. ===")
    return all_rh_rent_data

# 병합된 딕셔너리 리스트를 문자열로 반환 함수
def return_rh_rent_string(data: list[dict]) -> list[dict]:   
    result_string: list[str] = []
    for record in data:
        record_str = (
            f"지역코드: {str(record.get('sggCd',''))}\n"
            f"법정동명: {str(record.get('umdNm',''))}\n"
            f"연립다세대명: {str(record.get('mhouseNm', ''))}\n"
            f"지번: {str(record.get('jibun', ''))}\n"
            f"건축년도: {str(record.get('buildYear', ''))}\n"
            f"전용면적(㎡): {str(record.get('excluUseAr',''))}\n"
            f"계약년도: {str(record.get('dealYear',''))}\n"
            f"계약월: {str(record.get('dealMonth',''))}\n"
            f"계약일: {str(record.get('dealDay',''))}\n"
            f"보증금액(만원): {str(record.get('deposit','')).replace(',', '')}\n"
            f"월세금액(만원): {str(record.get('monthlyRent','')).replace(',', '')}\n"
            f"층: {str(record.get('floor', ''))}\n"
            f"계약기간: {str(record.get('contractTerm',''))}\n"
            f"계약구분: {str(record.get('contractType',''))}\n"
            f"갱신요구권사용: {str(record.get('useRRRight',''))}\n"
            f"종전계약보증금(만원): {str(record.get('preDeposit',''))}\n"
            f"종전계약월세(만원): {str(record.get('preMonthlyRent',''))}\n"
        )

        last_data = {
            "metadata": {
                "REGION_CODE": record.get('sggCd',''),
                "ENACTMENT_DATA": f"{record.get('dealYear','')}{record.get('dealMonth','')}{record.get('dealDay','')}"
            },
            "content": record_str
        }

        result_string.append(last_data)

    return result_string

# txt 파일로 저장하는 함수
def save_rh_rent_data_to_txt() -> None:
    # 날짜 설정
    now = datetime.datetime.now()
    year = now.year
    month = now.month
    day = now.day
    ym = f"{year}{month:02d}"
    filedate = f"{year}{month:02d}{day:02d}" # 파일명에 사용할 날짜 문자열 설정 -> YYYYMMDD
    # 데이터 수집
    all_rh_rent_data = get_all_rh_rent_data(ym=ym)

    # 문자열 리스트로 변환
    rh_rent_strings = return_rh_rent_string(all_rh_rent_data)

    # 파일명 설정
    filename = f"txts/rh_real_estate/rh_rent_data_{filedate}.txt"

    # 중복 제거 로직 시작
    yesterday = now - datetime.timedelta(days=1)
    yesterday_filedate = f"{yesterday.year}{yesterday.month:02d}{yesterday.day:02d}"    
    yesterday_filepath = f"txts/rh_real_estate/rh_rent_data_{yesterday_filedate}.txt"    
    previous_hashes = load_previous_hashes(yesterday_filepath)
    print(f"=== 이전 파일에서 {len(previous_hashes)}개의 중복 해시 로드 완료 ===")  
    
    filtered_list: list[dict] = []

    for rent in rh_rent_strings:
        content = rent.get("content", "")
        content_hash = md5_hash(content)
        if content_hash not in previous_hashes:
            filtered_list.append(rent)
    print(f"=== 중복 제거 후 최종 저장할 데이터 건수: {len(filtered_list)}건 ===")
    # 중복 제거 로직 끝

    # 텍스트 파일로 저장
    with open(filename, "w", encoding="utf-8") as f:
        for record in filtered_list:
            f.write(json.dumps(record, ensure_ascii=False))
            f.write("\n")  # 각 기록 사이에 줄바꿈 추가

    print(f"연립다세대 전월세 실거래가 데이터가 '{filename}' 파일로 저장되었습니다.")


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

schedule.every(1).days.do(save_rh_trade_data_to_txt)
schedule.every(1).days.do(save_rh_rent_data_to_txt)

# --- 스크립트 실행 (Main Pipeline) ---
if __name__ == "__main__":
    # print("=== 연립다세대 매매 실거래가 데이터 수집 테스트 ===\n")
    # save_rh_trade_data_to_txt()
    # print("\n=== 연립다세대 전월세 실거래가 데이터 수집 테스트 ===\n")
    # save_rh_rent_data_to_txt()

    while True:
        schedule.run_pending()
        # 30분 대기
        time.sleep(1800)