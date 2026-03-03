# 토지 거래 실거래가 데이터 수집 모듈 이거로 사용할 예정 

import requests
import xmltodict
import os
import dotenv
import datetime
from dataPortal import region
import json
import glob
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

    print(f"=== {ym} 경기도 토지 매매 데이터 수집 시작 ===")

    region_dict = region.get_all_sgg_code_dict()
    total_regions = len(region_dict)

    for i, (region_name, lawd_cd) in enumerate(region_dict.items()):
        
        print(f" - [{i+1}/{total_regions}] {region_name} ({lawd_cd}) 데이터 수집 중...")
        
        rent_data = land_trade(lawd_cd, ym) 
        
        if rent_data:
            all_land_data.extend(rent_data)
        else:
            print(f"    -> {region_name} 지역은 법정동코드({lawd_cd}), {ym}월 매매 거래 내역이 없습니다.")

    print(f"\n=== 모든 지역 데이터 병합 완료. {ym} 경기도 총 토지 매매 데이터: {len(all_land_data)}건 ===")
    
    return all_land_data

# 토지 거래 데이터 리스트를 문자열 리스트로 변환 함수 (RAG 최적화 적용)
def return_land_trade_string(data: list[dict]) -> list[dict]:
    result_strings: list[dict] = []
    
    # 데이터가 없으면 빈 리스트 반환
    if not data:
        return []

    print(f"\n=== {len(data)}건의 토지 데이터를 RAG 포맷으로 변환 시작 ===")

    for record in data:
        try:
            # --- 헬퍼: 값 안전하게 가져오기 ---
            def get_val(key, default=''):
                val = record.get(key)
                return str(val).strip() if val is not None else default

            # --- 1. 수치 데이터 전처리 (금액 & 평수) ---
            # 금액 포맷팅 (만원 단위 입력 -> 억/만원 변환)
            raw_amount = get_val('dealAmount', '0').replace(',', '')
            try:
                amt_int = int(raw_amount)
                if amt_int == 0:
                    price_str = "0원"
                elif amt_int >= 10000:
                    eok = amt_int // 10000
                    man = amt_int % 10000
                    price_str = f"{eok}억원" if man == 0 else f"{eok}억 {man:,}만원"
                else:
                    price_str = f"{amt_int:,}만원"
            except:
                price_str = "가격정보없음"

            # 면적 포맷팅 (m2 -> 평 환산)
            raw_area = get_val('dealArea', '0')
            try:
                area_float = float(raw_area)
                pyeong = round(area_float / 3.3058, 1)
            except:
                area_float = 0.0
                pyeong = 0.0

            # --- 2. 날짜 및 주소 ---
            year = get_val('dealYear')
            month = get_val('dealMonth').zfill(2)
            day = get_val('dealDay').zfill(2)
            deal_date = f"{year}년 {month}월 {day}일"

            # 주소 조합
            sgg = get_val('sggNm')
            umd = get_val('umdNm')
            jibun = get_val('jibun')
            full_address = f"{sgg} {umd} {jibun}번지"
            dealing_type = get_val('dealingGbn')

            # 중개사소재지
            estate_agent_sgg = get_val('estateAgentSggNm')

            cdeal_type = get_val('cdealType')

            # --- 3. 문장 구성 (RAG 최적화: 키워드 위주) ---
            # 불필요한 서술어 제거, 핵심 정보 구조화
            # 자연어 문장으로 처리 (ex) 거래일자: 2025년 12월 3일, 위치: 경기 광주시 법정동 초월읍 쌍동리 (지번: 395)에 위치한 '초월역모아미래도파크힐스' 아파트 103동동 8층 매물이 거래금액 4억 6000만원에 중개거래되었습니다.
            main_content = (
                f"토지 매매 거래입니다. 거래일자는 {deal_date}입니다. "
                # f"위치: {full_address} | "
                # f"지목: '{get_val('jimok')}' | 용도: '{get_val('landUse')}' | "
                # f"면적: {area_float}㎡ (약 {pyeong}평) | 금액: {price_str} | 유형: '{get_val('dealingGbn')}'"
                f"{full_address}에 위치한 지목 '{get_val('jimok')}', 용도지역 '{get_val('landUse')}' 토지 매물이 {price_str}에 {dealing_type}되었습니다."
                f" 면적은 {area_float}㎡ (약 {pyeong}평)입니다."
            )

            if dealing_type == "중개거래":
                main_content += " 중개사소재지는 " + estate_agent_sgg + "입니다."    
            # --- 4. 추가 정보 (해제여부 등) ---
            if cdeal_type == "O":
                cdeal_day = get_val('cdealDay')# 해제사유발생일 25.01.15 -> 2025년 01월 15일 포맷 변환

                if cdeal_day:
                    cdeal_day_formatted = f"20{cdeal_day[:2]}년 {cdeal_day[3:5]}월 {cdeal_day[6:8]}일"
                    main_content += f" 이 거래는 {cdeal_day_formatted}에 해제되었습니다."
                else:
                    main_content += " 이 거래는 해제되었습니다."


            # --- 5. 결과 저장 ---
            last_data = {
                "metadata": {
                    "region_code": get_val('sggCd'),
                    "enactment_date": f"{year}{month}{day}"
                },
                "content": main_content
            }

            result_strings.append(last_data)

        except Exception as e:
            # 에러 발생 시 로그 출력하고 건너뜀 (전체 프로세스 중단 방지)
            print(f"변환 중 에러 발생: {e} | 지역: {record.get('sggNm')} {record.get('umdNm')}")
            continue

    return result_strings


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

# txt 파일로 저장하는 함수
def save_land_trade_data_to_txt() -> None:

    # 현재 날짜 기준 연월(YYYYMM) 설정
    now = datetime.datetime.now()
    year = now.year
    month = now.month
    day = now.day
    ym = f"{year}{month:02d}"
    prev_ym = f"{year}{month-1:02d}" if month > 1 else f"{year-1}12"
    
    # 전체 토지 데이터 조회
    total_data : list[dict] = get_all_land_trade_data(ym) + get_all_land_trade_data(prev_ym) # 이번달과 지난달 데이터 모두 수집하여 병합

    # 데이터가 없으면 종료
    if not total_data:
        print(f"=== {ym} 또는 {prev_ym} 기간에 조회된 전원세 데이터가 전혀 없습니다. ===")
        return
    
    text_strings: list[dict] = return_land_trade_string(total_data)

    # ######################중복 로직 추가 
    previous_hashes = set()
    folder_path = "txts/land_real_estate"
    os.makedirs(folder_path, exist_ok=True)  # 폴더가 없으면 생성

    for file in glob.glob(os.path.join(folder_path, f"land_data_{ym}*.txt")) + glob.glob(os.path.join(folder_path, f"land_data_{prev_ym}*.txt")):  # 이번달과 지난달 파일 패턴과 일치하는 기존 파일들에서 해시 로드
        file_hashes = load_previous_hashes(file)
        previous_hashes.update(file_hashes)

    print(f"=== 이전 파일에서 {len(previous_hashes)}개의 해시 로드 완료 ===")
    # 중복 제거
    filtered_list: list[dict] = []
    for record in text_strings:
        content = record.get("content", "")
        content_hash = md5_hash(content)
        if content_hash not in previous_hashes:
            filtered_list.append(record)
    print(f"=== 중복 제거 후 최종 저장할 데이터 건수: {len(filtered_list)}건 ===")
############# 중복 로직 끝 ######################

    if len(filtered_list) == 0:
        print("=== 신규 데이터가 0건이므로 파일 저장을 수행하지 않습니다. ===")
        return
    
    # 파일 저장
    filedate = f"{year}{month:02d}{day:02d}"
    filename = f"txts/land_real_estate/land_data_{filedate}.txt"
    with open(filename, 'w', encoding='utf-8') as f:
        for text in filtered_list:
            f.write(json.dumps(text, ensure_ascii=False))
            f.write("\n")

    print(f"텍스트 파일로 저장 완료: {filename}")


if __name__ == "__main__":
    # 토지 매매 실거래가 데이터 txt 파일로 저장 
    # 스케줄 실행
    save_land_trade_data_to_txt()