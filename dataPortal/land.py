# 토지 거래 실거래가 데이터 수집 모듈

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

# 토지 거래 API 호출 함수
def land_trade(lawd_code: str, deal_ym: str, n_rows: int = 9999) -> list[dict]:
    # https://www.data.go.kr/data/15126469/openapi.do
    def _api_call(lawd_code: str, deal_ym: str, n_rows: int, page: int) -> dict:
        url = "https://apis.data.go.kr/1613000/RTMSDataSvcLandTrade/getRTMSDataSvcLandTrade"
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
        
# 
def get_all_land_trade_data(ym: str) -> list[dict]:

    all_land_data = []

    print(f"=== {ym} 전국 아파트 전원세 데이터 수집 시작 ===")

    region_dict = region.get_all_sgg_code_dict()
    total_regions = len(region_dict)

    for i, (region_name, lawd_cd) in enumerate(region_dict.items()):
        
        print(f" - [{i+1}/{total_regions}] {region_name} ({lawd_cd}) 데이터 수집 중...")
        
        rent_data = land_trade(lawd_cd, ym) 
        
        if rent_data:
            all_land_data.extend(rent_data)
        else:
            print(f"    -> {region_name} 지역은 법정동코드({lawd_cd}), {ym}월 전원세 거래 내역이 없습니다.")

    print(f"\n=== 모든 지역 데이터 병합 완료. {ym} 전국 총 전원세 데이터: {len(all_land_data)}건 ===")
    
    return all_land_data

# 토지 거래 데이터 리스트를 문자열 리스트로 변환 함수
def return_land_trade_string(data: list[dict]) -> list[str]:
    result_strings: list[str] = []
    for record in data:
        record_str = (
            f"지역코드: {str(record.get('sggCd',''))}\n"
            f"시군구: {str(record.get('sggNm',''))}\n"
            f"법정동명: {str(record.get('umdNm',''))}\n"
            f"지번: {str(record.get('jibun',''))}\n"
            f"지목: {str(record.get('jimok',''))}\n"
            f"용도지역: {str(record.get('landUse',''))}\n"
            f"계약년도: {str(record.get('dealYear',''))}\n"
            f"계약월: {str(record.get('dealMonth',''))}\n"
            f"계약일: {str(record.get('dealDay',''))}\n"
            f"거래면적(㎡): {str(record.get('dealArea',''))}\n"
            f"거래금액(만원): {str(record.get('dealAmount','')).replace(',', '')}\n"
            f"지분거래구분: {str(record.get('shareDealingType',''))}\n"
            f"해제여부: {str(record.get('cdealType',''))}\n"
            f"해제사유발생일: {str(record.get('cdealDay',''))}\n"
            f"거래유형: {str(record.get('dealingGbn',''))}\n"
            f"중개사소재지: {str(record.get('estateAgentSggNm',''))}\n"
        )
        result_strings.append(record_str)
    return result_strings

# txt 파일로 저장하는 함수
def save_land_trade_data_to_txt() -> None:

    # 현재 날짜 기준 연월(YYYYMM) 설정
    now = datetime.datetime.now()
    year = now.year
    month = now.month
    day = now.day
    ym = f"{year}{month:02d}"
    # 전체 토지 데이터 조회
    total_data : list[dict] = get_all_land_trade_data(ym)

    # 데이터가 없으면 종료
    if not total_data:
        print(f"=== {ym} 기간에 조회된 전원세 데이터가 전혀 없습니다. ===")
        return
    
    text_strings: list[str] = return_land_trade_string(total_data)

    filedate = f"{year}{month:02d}{day:02d}" # 파일명에 사용할 날짜 문자열 설정 -> YYYYMMDD
    filename = f"txts/land_real_estate/land_data_{filedate}.txt" # 파일명 설정 -> real_estate/land_trade_data_YYYYMMDD.txt
    with open(filename, 'w', encoding='utf-8') as f:
        for i, text in enumerate(text_strings):
            f.write(text) # 텍스트 쓰기
            f.write("\n") # 각 문서 구분을 위한 빈 줄 추가
    print(f"텍스트 파일로 저장 완료: {filename}")

if __name__ == "__main__":
    save_land_trade_data_to_txt()