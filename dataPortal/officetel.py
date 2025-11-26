# 오피스텔 매매, 전월세 실거래가 데이터 수집 모듈

import requests
import xmltodict
import os
import sys
import dotenv
import datetime
import region

# 초기화
dotenv.load_dotenv()
DATAGO_KEY = os.getenv("DATAGO_KEY")

# 오피스텔 매매 거래 API 호출 함수
def officetel_trade(lawd_code: str, deal_ym: str, n_rows: int = 9999) -> list[dict]:
    def _api_call(lawd_code: str, deal_ym: str, n_rows: int, page: int) -> dict:
        url = "https://apis.data.go.kr/1613000/RTMSDataSvcOffiTrade/getRTMSDataSvcOffiTrade"
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

# 오피스텔 매매 실거래가 전체 반환 함수
def get_all_officetel_trade_data(ym: str) -> list[dict]:

    all_officetel_data = []
    # 전국 '시/군/구' 법정동 코드 딕셔너리 조회
    sgg_dict = region.get_all_sgg_code_dict()

    for region_name, lawd_code in sgg_dict.items():
        try:
            officetel_data = officetel_trade(lawd_code=lawd_code, deal_ym=ym, n_rows=9999)
            all_officetel_data.extend(officetel_data)
            print(f"[{region_name} - {lawd_code}] 지역 오피스텔 거래 데이터 {len(officetel_data)} 건 수집 완료.")
        except Exception as e:
            print(f"Error fetching officetel data for {region_name} ({lawd_code}): {e}")
    
    return all_officetel_data

# 병합된 딕셔너리 리스트를 문자열로 반환 함수
def return_officetel_string(data: list[dict]) -> list[str]:
    result_string: list[str] = []
    for record in data:
        record_str = (
            f"지역코드: {str(record.get('sggCd',''))}\n"
            f"시군구: {str(record.get('sggNm',''))}\n"
            f"법정동명: {str(record.get('umdNm',''))}\n"
            f"지번: {str(record.get('jibun',''))}\n"
            f"단지명: {str(record.get('offiNm', ''))}\n"
            f"전용면적(㎡): {str(record.get('excluUseAr', ''))}\n"
            f"계약년도: {str(record.get('dealYear',''))}\n"
            f"계약월: {str(record.get('dealMonth',''))}\n"
            f"계약일: {str(record.get('dealDay',''))}\n"
            f"거래금액(만원): {str(record.get('dealAmount','')).replace(',', '')}\n"
            f"층: {str(record.get('floor', ''))}\n"
            f"건축년도: {str(record.get('buildYear', ''))}\n"
            f"해제여부: {str(record.get('cdealType',''))}\n"
            f"해제사유발생일: {str(record.get('cdealDay',''))}\n"
            f"거래유형: {str(record.get('dealingGbn',''))}\n"
            f"중개사소재지: {str(record.get('estateAgentSggNm',''))}\n"
            f"매도자: {str(record.get('slerGbn', ''))}\n"
            f"매수자: {str(record.get('buyerGbn', ''))}\n"
        )
        result_string.append(record_str)

    return result_string

# 오피스텔 매매 실거래가 데이터를 텍스트 파일로 저장하는 함수
def save_officetel_trade_data_to_txt() -> None:
    # 날짜 설정
    now = datetime.datetime.now() # 현재 날짜 -> 형식 YYYY-MM-DD
    year = now.year
    month = now.month
    day = now.day
    ym = f"{year}{month:02d}"
    filedate = f"{year}{month:02d}{day:02d}" # 파일명에 사용할 날짜 문자열 설정 -> YYYYMMDD

    officetel_data = get_all_officetel_trade_data(ym)
    officetel_strings = return_officetel_string(officetel_data)
    # txts/officetel_real_estate/ 폴더에 저장
    filename = f"txts/officetel_real_estate/officetel_data_{filedate}.txt"
    # 텍스트 파일로 생성해서 저장
    with open(filename, "w", encoding="utf-8") as f:
        for record_str in officetel_strings:
            f.write(record_str + "\n")
    
    print(f"오피스텔 거래 데이터가 '{filename}' 파일로 저장되었습니다.")

# 오피스텔 전월세 거래 API 호출 함수
def officetel_rent_trade(lawd_code: str, deal_ym: str, n_rows: int = 9999) -> list[dict]:
    def _api_call(lawd_code: str, deal_ym: str, n_rows: int, page: int) -> dict:
        url = "https://apis.data.go.kr/1613000/RTMSDataSvcOffiRent/getRTMSDataSvcOffiRent"
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
        
# 오피스텔 전월세 실거래가 전체 반환 함수
def get_all_officetel_rent_data(ym: str) -> list[dict]:
    
    all_officetel_rent_data = []
    # 전국 '시/군/구' 법정동 코드 딕셔너리 조회
    sgg_dict = region.get_all_sgg_code_dict()

    for region_name, lawd_code in sgg_dict.items():
        try:
            officetel_rent_data = officetel_rent_trade(lawd_code=lawd_code, deal_ym=ym, n_rows=9999)
            all_officetel_rent_data.extend(officetel_rent_data)
            print(f"[{region_name} - {lawd_code}] 지역 오피스텔 전월세 거래 데이터 {len(officetel_rent_data)} 건 수집 완료.")
        except Exception as e:
            print(f"Error fetching officetel rent data for {region_name} ({lawd_code}): {e}")
    
    return all_officetel_rent_data

# 병합된 딕셔너리 리스트를 문자열로 반환 함수
def return_officetel_rent_string(data: list[dict]) -> list[str]:
    result_string: list[str] = []
    for record in data:
        record_str = (
            f"지역코드: {str(record.get('sggCd',''))}\n"
            f"시군구: {str(record.get('sggNm',''))}\n"
            f"법정동명: {str(record.get('umdNm',''))}\n"
            f"지번: {str(record.get('jibun',''))}\n"
            f"단지명: {str(record.get('offiNm', ''))}\n"
            f"전용면적(㎡): {str(record.get('excluUseAr', ''))}\n"
            f"계약년도: {str(record.get('dealYear',''))}\n"
            f"계약월: {str(record.get('dealMonth',''))}\n"
            f"계약일: {str(record.get('dealDay',''))}\n"
            f"보증금액(만원): {str(record.get('deposit','')).replace(',', '')}\n"
            f"월세금액(만원): {str(record.get('monthlyRent','')).replace(',', '')}\n"
            f"층: {str(record.get('floor', ''))}\n"
            f"건축년도: {str(record.get('buildYear', ''))}\n"
            f"계약기간: {str(record.get('contractTerm',''))}\n"
            f"갱신요구권사용: {str(record.get('useRRRight',''))}\n"
            f"종전계약보증금(만원): {str(record.get('preDeposit',''))}\n"
            f"종전계약월세(만원): {str(record.get('preMonthlyRent',''))}\n"
        )
        result_string.append(record_str)

    return result_string

# 오피스텔 전월세 실거래가 데이터를 텍스트 파일로 저장하는 함수
def save_officetel_rent_data_to_txt() -> None:
    # 날짜 설정
    now = datetime.datetime.now() # 현재 날짜 -> 형식 YYYY-MM-DD
    year = now.year
    month = now.month
    day = now.day
    ym = f"{year}{month:02d}"
    filedate = f"{year}{month:02d}{day:02d}" # 파일명에 사용할 날짜 문자열 설정 -> YYYYMMDD

    officetel_rent_data = get_all_officetel_rent_data(ym)
    officetel_rent_strings = return_officetel_rent_string(officetel_rent_data)

    filename = f"txts/officetel_real_estate/officetel_rent_data_{filedate}.txt"
    with open(filename, "w", encoding="utf-8") as f:
        for record_str in officetel_rent_strings:
            f.write(record_str + "\n")
    
    print(f"오피스텔 전월세 거래 데이터가 '{filename}' 파일로 저장되었습니다.")

if __name__ == "__main__":
    print("=== 오피스텔 매매 실거래가 데이터 수집 테스트 ===\n")
    save_officetel_trade_data_to_txt()
    print("\n=== 오피스텔 전월세 실거래가 데이터 수집 테스트 ===\n")
    save_officetel_rent_data_to_txt()