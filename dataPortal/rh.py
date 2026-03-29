# 연립다세대 매매/전월세 데이터 수집 모듈 

import time
import requests
import xmltodict
from dataPortal import region
import os 
import dotenv
import datetime
import json
import glob
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
    total_regions = len(region_dict)
    base_delay = 1.0  # 재시도 시 기본 지연 시간 (초)

    for i, (region_name, lawd_cd) in enumerate(region_dict.items()):
        print(f" - [{i+1}/{total_regions}] {region_name} ({lawd_cd}) 연립다세대 매매 데이터 수집 중...")
        for attempt in range(3):  # 최대 3회 재시도
            try:
                region_data = rh_trade(lawd_code=lawd_cd, deal_ym=ym)
                all_rh_data.extend(region_data)
                break  # 성공 시 루프 탈출
            except Exception as e:
                print(f"오류 발생 [{region_name} - {lawd_cd}] (시도 {attempt+1}/3): {e}")
                if attempt < 2:  # 마지막 시도가 아니면 지연 후 재시도
                    delay = base_delay * (2 ** attempt)  # 지수적 백오프
                    print(f"  -> {delay}초 후 재시도... ({attempt + 1}/3)")
                    time.sleep(delay)
                if attempt == 2:  # 마지막 시도에서도 실패한 경우
                    print(f"  -> {region_name} 지역 데이터 수집 실패 (오류: {e}). 다음 지역으로 넘어갑니다.")
    
    return all_rh_data

# 병합된 딕셔너리 리스트를 문자열로 반환 함수
def return_rh_trade_string(data: list[dict]) -> list[dict]:
    result_string: list[dict] = []

    def get_val(key, default=''):
            val = record.get(key)
            return str(val).strip() if val is not None else default

    def fmt_money(val_str):
            try:
                # 쉼표 제거 및 공백 제거
                clean_str = str(val_str).replace(',', '').strip()
                if not clean_str: return "0"
                    
                val = int(clean_str)
                if val == 0: return "0"
                    
                if val >= 10000:
                    eok = val // 10000
                    man = val % 10000
                    return f"{eok}억원" if man == 0 else f"{eok}억 {man}만원"
                return f"{val}만원"
            except:
                return "0원"

    for record in data:        
        year = get_val('dealYear')
        month = get_val('dealMonth').zfill(2)
        day = get_val('dealDay').zfill(2)
        deal_date = f"{year}년 {month}월 {day}일"

        dong = get_val('umdNm')
        jibun = get_val('jibun')
        mhouse_name = get_val('mhouseNm')
        floor = get_val('floor')
        
        if floor and floor.startswith('-'):
            floor_str = f"지하 {abs(int(floor))}층"
        else:
            floor_str = f"{floor}층" if floor else "층수미상"

        deal_amount = fmt_money(get_val('dealAmount'))
        build_year = get_val('buildYear')
        cdeal_type = get_val('cdealType') # 해제여부 ex O or None
        cdeal_date = get_val('cdealDay') # 해제사유발생일 ex 25.05.12 or '' to -> 2025년 05월 12일
        dealing_gbn = get_val('dealingGbn', "거래") # 거래유형 직거래 or 중개거래
        estate_agent_sgg_nm = get_val('estateAgentSggNm')
        rgst_date = get_val('rgstDate')
        sler_gbn = get_val('slerGbn')
        buyer_gbn = get_val('buyerGbn')
        house_type = get_val('houseType') # 주택유형 ex 연립 / 다세대
        area_raw = get_val('excluUseAr') # 전용면적
        land_area_raw = get_val('landAr') # 대지권면적
        
        if cdeal_date:
            try:
                cdeal_year, cdeal_month, cdeal_day = cdeal_date.split('.') # 25.5.12 형식에서 년, 월, 일 분리
                cdeal_date = f"20{int(cdeal_year)}년 {int(cdeal_month):02d}월 {int(cdeal_day):02d}일" # 2025년 05월 12일 형식으로 변환
            except:
                cdeal_date = cdeal_date

        try:
            area_float = float(area_raw)
            area_text = f"{area_float}㎡ (약 {area_float / 3.3058:.1f}평)"
        except:
            area_text = f"{area_raw}㎡ (약 {float(area_raw) / 3.3058:.1f}평)"
        
        try:
            land_area_float = float(land_area_raw)
            land_area_text = f"{land_area_float}㎡ (약 {land_area_float / 3.3058:.1f}평)"
        except:
            land_area_text = f"{land_area_raw}㎡ (정보없음)"

        record_str = (
            f"{house_type}주택 매매거래입니다. "
            f"거래일자는 {deal_date}입니다. "
            f"{dong} (지번: {jibun})에 위치한 "
            f"'{mhouse_name}' {house_type}주택 {floor_str} 매물이 "
            f"{deal_amount}에 {dealing_gbn}되었습니다. "
            f"전용면적은 {area_text}이고, 대지권면적은 {land_area_text}입니다. "
            f"건축년도는 {build_year}년입니다. "
            f"매도자구분은 {sler_gbn}이며, 매수자구분은 {buyer_gbn}입니다."
            )

        if dealing_gbn == '중개거래':
            record_str += f" 중개사소재지는 {estate_agent_sgg_nm}입니다."

        if cdeal_type == 'O':
            record_str += f" 해당 거래는 해제되었으며, 해제사유발생일은 {cdeal_date}입니다."

        if rgst_date:
            try:
                rgst_year, rgst_month, rgst_day = rgst_date.split('.')
                rgst_date_fmt = f"20{int(rgst_year)}년 {int(rgst_month):02d}월 {int(rgst_day):02d}일"
                record_str += f" 등기일자는 {rgst_date_fmt}입니다."
            except:
                continue
        
        last_data = {
            "metadata": {
                "region_code": record.get('sggCd',''),
                "enactment_date": f"{str(record.get('dealYear',''))}{str(record.get('dealMonth','')).zfill(2)}{str(record.get('dealDay','')).zfill(2)}"
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
    prev_ym = f"{year}{month-1:02d}" if month > 1 else f"{year-1}12"
    
    filedate = f"{year}{month:02d}{day:02d}" # 파일명에 사용할 날짜 문자열 설정 -> YYYYMMDD
    filedate = "20240101" # 테스트용으로 고정된 날짜 사용 (실제 운영 시에는 위의 동적 날짜 사용)
    # 데이터 수집
    # all_rh_data = get_all_rh_trade_data(ym=ym) + get_all_rh_trade_data(prev_ym) # 이번달과 지난달 데이터 모두 수집하여 병합
    all_rh_data = get_all_rh_trade_data("202401") + get_all_rh_trade_data("202402") + get_all_rh_trade_data("202403") + get_all_rh_trade_data("202404") + get_all_rh_trade_data("202405") + get_all_rh_trade_data("202406") + get_all_rh_trade_data("202407") + get_all_rh_trade_data("202408") + get_all_rh_trade_data("202409") + get_all_rh_trade_data("202410") + get_all_rh_trade_data("202411") + get_all_rh_trade_data("202412")
    print(f"=== 이번달과 지난달 연립다세대 매매 거래 데이터 총 {len(all_rh_data)}건 수집됨. ===")

    if not all_rh_data:
        print("=== 수집된 데이터가 없습니다. 파일 저장을 수행하지 않습니다. ===")
        return

    # 문자열 리스트로 변환
    rh_strings = return_rh_trade_string(all_rh_data)

    # 중복 제거 로직 시작
    previous_hashes = set()
    folder_path = "txts/rh_real_estate"
    os.makedirs(folder_path, exist_ok=True)  # 폴더가 없으면 생성

    for file in glob.glob(os.path.join(folder_path, f"rh_data_{ym}*.txt")) + glob.glob(os.path.join(folder_path, f"rh_data_{prev_ym}*.txt")): 
        file_hashes = load_previous_hashes(file)
        previous_hashes.update(file_hashes)

    print(f"=== 이전 파일에서 {len(previous_hashes)}개의 해시 로드 완료 ===")

    filtered_list: list[dict] = []

    for rent in rh_strings:
        content = rent.get("content", "")
        content_hash = md5_hash(content)
        if content_hash not in previous_hashes:
            filtered_list.append(rent)
    print(f"=== 중복 제거 후 최종 저장할 데이터 건수: {len(filtered_list)}건 ===")
    # 중복 제거 로직 끝

    if len(filtered_list) == 0:
        print("=== 신규 데이터가 0건이므로 파일 저장을 수행하지 않습니다. ===")
        return

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
    total_regions = len(region_dict)
    base_delay = 1.0  # 재시도 시 기본 지연 시간 (초)

    for i, (region_name, lawd_code) in enumerate(region_dict.items()):
        print(f" - [{i+1}/{total_regions}] {region_name} ({lawd_code}) 연립다세대 전월세 실거래가 데이터 수집 중...")
        for attempt in range(3):  # 최대 3회 재시도
            try:
                region_data = rh_rent_trade(lawd_code=lawd_code, deal_ym=ym)
                all_rh_rent_data.extend(region_data)
                break  # 성공 시 루프 탈출
            except Exception as e:
                print(f"오류 발생 [{region_name} - {lawd_code}] (시도 {attempt+1}/3): {e}")
                if attempt < 2:  # 마지막 시도가 아니면 지연 후 재시도
                    delay = base_delay * (2 ** attempt)  # 지수적 백오프
                    print(f"  -> {delay}초 후 재시도... ({attempt + 1}/3)")
                    time.sleep(delay)
                if attempt == 2:  # 마지막 시도에서도 실패한 경우
                    print(f"  -> {region_name} 지역 데이터 수집 실패 (오류: {e}). 다음 지역으로 넘어갑니다.")
    
    return all_rh_rent_data

# 병합된 딕셔너리 리스트를 문자열로 반환 함수
def return_rh_rent_string(data: list[dict]) -> list[dict]:   
    result_string: list[dict] = []

    def get_val(key, default=''):
            val = record.get(key)
            return str(val).strip() if val is not None else default

    def fmt_money(val_str):
            try:
                # 쉼표 제거 및 공백 제거
                clean_str = str(val_str).replace(',', '').strip() 
                if not clean_str: return "0" 
                    
                val = int(clean_str)
                if val == 0: return "0원"
                    
                if val >= 10000:
                    eok = val // 10000
                    man = val % 10000
                    return f"{eok}억원" if man == 0 else f"{eok}억 {man}만원"
                return f"{val}만원"
            except:
                return "0원"

    for record in data:
        year = get_val('dealYear')
        month = get_val('dealMonth').zfill(2)
        day = get_val('dealDay').zfill(2)
        deal_date = f"{year}년 {month}월 {day}일"

        dong = get_val('umdNm')
        jibun = get_val('jibun')
        mhouse_name = get_val('mhouseNm')
        house_type = get_val('houseType') # 주택유형 ex 연립 / 다세대
        floor = get_val('floor')

        if floor and floor.startswith('-'):
            floor_str = f"지하 {abs(int(floor))}층"
        else:
            floor_str = f"{floor}층" if floor else "층수미상"

        build_year = get_val('buildYear')
        contract_type = get_val('contractType')
        contract_term = get_val('contractTerm')
        use_rr_right = get_val('useRRRight')
        area_raw = get_val('excluUseAr') # 전용면적

        try:
            area_float = float(area_raw)
            area_text = f"{area_float}㎡ (약 {area_float / 3.3058:.1f}평)"
        except:
            area_text = f"{area_raw}㎡ (약 {float(area_raw) / 3.3058:.1f}평)"

        # --- 5. 계약 및 갱신 정보 ---
        contract_type = get_val('contractType', "신규") # 신규/갱신/공백 계약구분 
        term_raw = get_val('contractTerm')

        if term_raw and '~' in term_raw:
            try:
                start, end = term_raw.split('~')
                sy, sm = start.split('.')
                ey, em = end.split('.')
                term = f"20{sy}년 {sm.zfill(2)}월부터 20{ey}년 {em.zfill(2)}월까지"
            except:
                term = "정보없음"
        else:
            term = "정보없음"
            
        use_rr = get_val('useRRRight') # 사용/공백 갱신요구권
        if use_rr == '사용':
            pass
        else:
            use_rr = "미사용"

        # --- 3. 전/월세 금액 처리 ---
        dep_raw = get_val('deposit', '0')
        mon_raw = get_val('monthlyRent', '0')
            
        dep_fmt = fmt_money(dep_raw)
        mon_fmt = fmt_money(mon_raw)
        
        # 월세 여부 판단 (월세 금액이 0보다 크면 월세)
        try:
            mon_raw = get_val('monthlyRent', '0')   
            mon_int = int(mon_raw.replace(',', ''))
        except:
            mon_int = 0

        if mon_int > 0:
            deal_type = "월세"
            price_text = f"보증금 {dep_fmt}, 월세 {mon_fmt}"
        else:
            deal_type = "전세"
            price_text = f"전세금 {dep_fmt}"

        preDeposit = fmt_money(get_val('preDeposit','')) # 종전계약보증금 (전세 or 보증금 or 0)
        preMonthlyRent = fmt_money(get_val('preMonthlyRent','')) # 종전계약월세 (월세 or 0)
            
        record_str = (
            f"{house_type}주택 {deal_type} 거래입니다. "
            f"거래일자는 {deal_date}입니다. "
            f"{dong} (지번: {jibun})에 위치한 "
            f"'{mhouse_name}' {house_type}주택 {floor_str} 매물이 "
            f"{price_text}으로 "
            f"{'계약되었습니다.' if contract_type != '갱신' else '갱신 계약되었습니다.'} "
            f"전용면적은 {area_text}입니다. "
            f"건축년도는 {build_year}년입니다. "
            f"계약기간은 {term}입니다. "
            f"갱신요구권 사용여부는 {use_rr}입니다."
        )
           
        if preMonthlyRent != '0': # 종전계약월세가 있으면 월세
            record_str += f" 종전계약 정보는 보증금 {preDeposit}, 월세 {preMonthlyRent}입니다."
        elif preDeposit != '0': # 종전계약보증금이 있으면 전세
            record_str += f" 종전계약 전세금은 {preDeposit}입니다."
        else:
            pass

        last_data = {
            "metadata": {
                "region_code": record.get('sggCd',''),
                "enactment_date": f"{str(record.get('dealYear',''))}{str(record.get('dealMonth','')).zfill(2)}{str(record.get('dealDay','')).zfill(2)}"
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
    prev_ym = f"{year}{month-1:02d}" if month > 1 else f"{year-1}12"

    filedate = f"{year}{month:02d}{day:02d}" # 파일명에 사용할 날짜 문자열 설정 -> YYYYMMDD
    filedate = "20240101" # 테스트용으로 고정된 날짜 사용 (실제 운영 시에는 위의 동적 날짜 사용)
    # 데이터 수집
    # all_rh_rent_data = get_all_rh_rent_data(ym=ym) + get_all_rh_rent_data(ym=prev_ym) # 이번달과 지난달 데이터 모두 수집하여 병합
    all_rh_rent_data = get_all_rh_rent_data("202401") + get_all_rh_rent_data("202402") + get_all_rh_rent_data("202403") + get_all_rh_rent_data("202404") + get_all_rh_rent_data("202405") + get_all_rh_rent_data("202406") + get_all_rh_rent_data("202407") + get_all_rh_rent_data("202408") + get_all_rh_rent_data("202409") + get_all_rh_rent_data("202410") + get_all_rh_rent_data("202411") + get_all_rh_rent_data("202412")
    print(f"=== 이번달과 지난달 연립다세대 전월세 거래 데이터 총 {len(all_rh_rent_data)}건 수집됨. ===")

    if not all_rh_rent_data:
        print("=== 수집된 데이터가 없습니다. 파일 저장을 수행하지 않습니다. ===")
        return

    # 문자열 리스트로 변환
    rh_rent_strings = return_rh_rent_string(all_rh_rent_data)

    # 파일명 설정
    filename = f"txts/rh_real_estate/rh_rent_data_{filedate}.txt"

    # 중복 제거 로직 시작
    previous_hashes = set()
    folder_path = "txts/rh_real_estate"
    os.makedirs(folder_path, exist_ok=True)  # 폴더가 없으면 생성

    for file in glob.glob(os.path.join(folder_path, f"rh_rent_data_{ym}*.txt")) + glob.glob(os.path.join(folder_path, f"rh_rent_data_{prev_ym}*.txt")):  # 이번달과 지난달 파일 패턴과 일치하는 기존 파일들에서 해시 로드
        file_hashes = load_previous_hashes(file)
        previous_hashes.update(file_hashes)

    print(f"=== 이전 파일에서 {len(previous_hashes)}개의 해시 로드 완료 ===")  
    
    filtered_list: list[dict] = []

    for rent in rh_rent_strings:
        content = rent.get("content", "")
        content_hash = md5_hash(content)
        if content_hash not in previous_hashes:
            filtered_list.append(rent)
    print(f"=== 중복 제거 후 최종 저장할 데이터 건수: {len(filtered_list)}건 ===")
    # 중복 제거 로직 끝

    if len(filtered_list) == 0:
        print("=== 신규 데이터가 0건이므로 파일 저장을 수행하지 않습니다. ===")
        return

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


# --- 스크립트 실행 (Main Pipeline) ---
if __name__ == "__main__":
    # print("=== 연립다세대 매매 실거래가 데이터 수집 테스트 ===\n")
    # print("\n=== 연립다세대 전월세 실거래가 데이터 수집 테스트 ===\n")
    save_rh_trade_data_to_txt()
    save_rh_rent_data_to_txt()
