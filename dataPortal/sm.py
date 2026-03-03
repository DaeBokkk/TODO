# 단독/다가구 매매, 전월세 실거래가 데이터 수집 모듈

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
                    return f"{eok}억원" if man == 0 else f"{eok}억 {man:,}만원"
                return f"{val:,}만원"
            except:
                return "0원"

    for record in data:
        # record_str = (
        #     f"지역코드: {record.get('sggCd','')}\n"
        #     f"지역명: {record.get('umdNm','')}\n"
        #     f"주택유형(단독/다가구): {record.get('houseType','')}\n"
        #     f"지번: {record.get('jibun','')}\n"
        #     f"연면적(㎡): {record.get('totalFloorAr','')}\n"
        #     f"대지면적(㎡): {record.get('plottageAr','')}\n"
        #     f"계약년: {record.get('dealYear','')}\n"
        #     f"계약월: {record.get('dealMonth','')}\n"
        #     f"계약일: {record.get('dealDay','')}\n"
        #     f"거래금액(만원): {str(record.get('dealAmount','')).replace(',', '')}\n"
        #     f"건축년도: {record.get('buildYear','')}\n"
        #     f"해제여부: {record.get('cdealType','')}\n"
        #     f"해제사유발생일: {record.get('cdealDay','')}\n"
        #     f"거래유형(중개 및 직거래 여부): {record.get('dealingGbn','')}\n"
        #     f"중개사소재지(시군구 단위): {record.get('estateAgentSggNm','')}\n"
        #     f"거래주체정보_매도자(개인/법인/공공기관/기타): {record.get('slerGbn','')}\n"
        #     f"거래주체정보_매수자(개인/법인/공공기관/기타): {record.get('buyerGbn','')}\n"
        # )

        year = get_val('dealYear', '')
        month = get_val('dealMonth', '').zfill(2)
        day = get_val('dealDay', '').zfill(2)
        deal_date = f"{year}년 {month}월 {day}일"
        
        deal_amount = fmt_money(get_val('dealAmount', '0'))
        dong = get_val('umdNm', '')
        house_type = get_val('houseType', '')
        build_year = get_val('buildYear', '')
        jibun = get_val('jibun', '')

        plottage_ar = get_val('plottageAr', '') # 대지면적
        total_floor_ar = get_val('totalFloorAr', '') # 연면적

        dealing_gbn = get_val('dealingGbn', '거래') # 중개 및 직거래 여부
        sler_gbn = get_val('slerGbn', '기타') # 매도자 구분
        buyer_gbn = get_val('buyerGbn', '기타') # 매수자 구분

        # 평수 환산
        try:
            plottage_float = float(plottage_ar)
            plottage_text = f"{plottage_float}㎡ (약 {plottage_float / 3.3058:.1f}평)"
        except:
            plottage_text = f"{plottage_ar}㎡"

        try:
            total_floor_float = float(total_floor_ar)
            total_floor_text = f"{total_floor_float}㎡ (약 {total_floor_float / 3.3058:.1f}평)"
        except:
            total_floor_text = f"{total_floor_ar}㎡"

        record_str = (
            f"{house_type}주택 매매 거래입니다. "
            f"거래일자는 {deal_date} 입니다. "
            f"{dong} (지번: {jibun})에 위치한 "
            f"{house_type}주택 매물이 {deal_amount}에 {dealing_gbn}되었습니다. "
            f"연면적 {total_floor_text}, 대지면적 {plottage_text}입니다. "
            f"건축년도는 {build_year}년입니다. "
            f"매도자 구분은 {sler_gbn}, 매수자 구분은 {buyer_gbn}입니다."
        )

        if dealing_gbn == "중개거래":
            estate_agent_sgg_nm = get_val('estateAgentSggNm', '')
            record_str += f" 중개사소재지는 {estate_agent_sgg_nm}입니다."
        
        cdeal_type = get_val('cdealType', '')

        if cdeal_type == "O":  # 해제여부가 'O'인 경우에만 해제사유발생일 추가
            cdeal_day = get_val('cdealDay', '') # 해제사유발생일 ex 25.01.12 -> 2025년 01월 12일
            cdeal_year, cdeal_month, cdeal_day_part = cdeal_day.split('.')
            cdeal_day_formatted = f"20{cdeal_year}년 {cdeal_month}월 {cdeal_day_part}일"
            record_str += f" 이 거래는 해제된 거래로, 해제사유발생일은 {cdeal_day_formatted}입니다."

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
def save_sm_trade_data_to_txt() -> None:
    
    now = datetime.datetime.now()
    year = now.year
    month = now.month
    day = now.day
    ym = f"{year}{month:02d}"
    prev_ym = f"{year}{month-1:02d}" if month > 1 else f"{year-1}12"

    total_df: list[dict] = get_all_sm_trade_data(ym) + get_all_sm_trade_data(prev_ym) # 이번달과 지난달 데이터 모두 수집하여 병합
    
    if not total_df:
        print(f"=== {ym} 또는 {prev_ym} 기간에 조회된 단독/다가구 매매 실거래가 데이터가 전혀 없습니다. ===")
        return

    text_strings: list[dict] = return_sm_trade_string(total_df)

    # 중복 제거 로직 추가
    previous_hashes = set()
    folder_path = "txts/sm_real_estate"
    os.makedirs(folder_path, exist_ok=True)  # 폴더가 없으면 생성

    for file in glob.glob(os.path.join(folder_path, f"sm_data_{ym}*.txt")) + glob.glob(os.path.join(folder_path, f"sm_data_{prev_ym}*.txt")): # 이번달과 지난달 파일 패턴과 일치하는 기존 파일들에서 해시 로드
        file_hashes = load_previous_hashes(file)
        previous_hashes.update(file_hashes)

    print(f"=== 이전 파일에서 {len(previous_hashes)}개의 해시 로드 완료 ===")

    filtered_list: list[dict] = []
    for rent in text_strings:
        content = rent.get("content", "")
        content_hash = md5_hash(content)
        if content_hash not in previous_hashes:
            filtered_list.append(rent)
    print(f"=== 중복 제거 후 최종 저장할 데이터 건수: {len(filtered_list)}건 ===")
    ############# 중복 로직 끝 ######################

    if len(filtered_list) == 0:
        print("=== 신규 데이터가 0건이므로 파일 저장을 수행하지 않습니다. ===")
        return

    filedate = f"{year}{month:02d}{day:02d}" # 파일명에 사용할 날짜 문자열 설정 -> YYYYMMDD
    filename = f"txts/sm_real_estate/sm_data_{filedate}.txt" # 파일명 설정 -> real_estate/sm_documents_YYYYMMDD.txt

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
                    return f"{eok}억원" if man == 0 else f"{eok}억 {man:,}만원"
                return f"{val:,}만원"
            except:
                return "0원"

    for record in data:
        # record_str = (
        #     f"지역코드: {record.get('sggCd','')}\n"
        #     f"지역명: {record.get('umdNm','')}\n"
        #     f"주택유형(단독/다가구): {record.get('houseType','')}\n"
        #     f"연면적(㎡): {record.get('totalFloorAr','')}\n"
        #     f"계약년: {record.get('dealYear','')}\n"
        #     f"계약월: {record.get('dealMonth','')}\n"
        #     f"계약일: {record.get('dealDay','')}\n"
        #     f"보증금액(만원): {str(record.get('deposit','')).replace(',', '')}\n"
        #     f"월세금액(만원): {str(record.get('monthlyRent','')).replace(',', '')}\n"
        #     f"건축년도: {record.get('buildYear','')}\n"
        #     f"계약기간: {record.get('contractTerm','')}\n"
        #     f"계약구분: {record.get('contractType','')}\n"
        #     f"갱신요구권사용: {record.get('useRRRight','')}\n"
        #     f"종전계약보증금(만원): {str(record.get('preDeposit','')).replace(',', '')}\n"
        #     f"종전계약월세(만원): {str(record.get('preMonthlyRent','')).replace(',', '')}\n"
        # )

        year = get_val('dealYear','')
        month = get_val('dealMonth','').zfill(2)
        day = get_val('dealDay','').zfill(2)
        deal_date = f"{year}년 {month}월 {day}일"

        dong = get_val('umdNm','')
        house_type = get_val('houseType','')
        build_year = get_val('buildYear','')
    
        use_rr = get_val('useRRRight','')

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

        # --- 4. 면적 (평수 환산 포함) ---
        area_raw = get_val('totalFloorAr', '0')
        try:
            area_float = float(area_raw)
            area_text = f"{area_float}㎡ (약 {area_float / 3.3058:.1f}평)"
        except:
            area_text = f"{area_raw}㎡ (약 {float(area_raw) / 3.3058:.1f}평)"

        # --- 5. 계약 및 갱신 정보 ---
        contract_type = get_val('contractType', '신규') # 신규/갱신/공백 계약구분 
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

        # 갱신일 경우 종전 계약 정보 구성

        record_str = (
            f"{house_type}주택 {deal_type} 거래입니다. "
            f"거래일자는 {deal_date} 입니다. "
            f"{dong}에 위치한 "
            f"{house_type}주택 매물이 "
            f"{price_text}에 {contract_type}거래 되었습니다. "
            f"건축년도는 {build_year}년이며, 연면적은 {area_text}입니다. "
            f"갱신요구권은 {'사용' if use_rr == '사용' else '미사용'} 상태입니다. "
            f"계약기간은 {term}입니다."
            # f"거래일자: {deal_date
            # f"법정동: {dong} | "
            # f"주택유형: {house_type} | "
            # f"건축년도: {build_year}년 | "
            # f"연면적: {area_text} | "
            # f"거래유형: {deal_type} | "
            # f"거래금액: {price_text} | "
            # f"갱신요구권: {'사용' if use_rr == '사용' else '미사용'} | "
            # f"계약구분: {contract_type or '정보없음'} | "
            # f"계약기간: {term}"
        )
            
        if contract_type == "갱신":
            pre_dep_fmt = fmt_money(get_val('preDeposit', '0'))
            pre_mon_fmt = fmt_money(get_val('preMonthlyRent', '0'))
                
            # 종전 월세가 있는지 확인
            try:
                pre_mon_int = int(get_val('preMonthlyRent', '0').replace(',', ''))
            except:
                pre_mon_int = 0

            if pre_mon_int > 0: # 종전 월세가 있으면 월세
                record_str += f" 종전계약보증금은 {pre_dep_fmt}, 종전계약월세는 {pre_mon_fmt}입니다."
            elif pre_dep_fmt != '0':
                record_str += f" 종전계약전세금은 {pre_dep_fmt}입니다."
            else:
                record_str += " 종전계약정보가 없습니다."

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
    prev_ym = f"{year}{month-1:02d}" if month > 1 else f"{year-1}12"

    total_df: list[dict] = get_all_sm_trade_data(ym) + get_all_sm_trade_data(prev_ym) # 이번달과 지난달 데이터 모두 수집하여 병합
    
    if not total_df:
        print(f"=== {ym} 또는 {prev_ym} 기간에 조회된 단독/다가구 전월세 실거래가 데이터가 전혀 없습니다. ===")
        return
    
    text_strings: list[dict] = return_sm_rent_string(total_df)

    # 중복 제거 로직 추가
    previous_hashes = set()
    folder_path = "txts/sm_real_estate"
    os.makedirs(folder_path, exist_ok=True)  # 폴더가 없으면 생성

    for file in glob.glob(os.path.join(folder_path, f"sm_rent_data_{ym}*.txt")) + glob.glob(os.path.join(folder_path, f"sm_rent_data_{prev_ym}*.txt")): 
        file_hashes = load_previous_hashes(file)
        previous_hashes.update(file_hashes)

    print(f"=== 이전 파일에서 {len(previous_hashes)}개의 해시 로드 완료 ===")  

    filtered_list: list[dict] = []
    
    for rent in text_strings:
        content = rent.get("content", "")
        content_hash = md5_hash(content)
        if content_hash not in previous_hashes:
            filtered_list.append(rent)
    print(f"=== 중복 제거 후 최종 저장할 데이터 건수: {len(filtered_list)}건 ===")
    ############# 중복 로직 끝 ######################

    if len(filtered_list) == 0:
        print("=== 신규 데이터가 0건이므로 파일 저장을 수행하지 않습니다. ===")
        return

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


# --- 스크립트 실행 (Main Pipeline) ---
if __name__ == "__main__":
    # # 단독/다가구 매매 실거래가 데이터 txt 파일로 저장
    save_sm_trade_data_to_txt()
    
    # # 단독/다가구 전월세 실거래가 데이터 txt 파일로 저장
    save_sm_rent_data_to_txt()