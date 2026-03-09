# 오피스텔 매매, 전월세 실거래가 데이터 수집 모듈

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
def return_officetel_string(data: list[dict]) -> list[dict]:
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
                    return f"{eok}억원" if man == 0 else f"{eok}억 {man:}만원"                    
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
        offi_name = get_val('offiNm')
        floor = get_val('floor')
        floor_str = f"{floor}층" if floor else "층수미상"
        build_year = get_val('buildYear')
        deal_amount = fmt_money(get_val('dealAmount', '0'))
        
        # --- 4. 면적 (평수 환산 포함) ---
        area_raw = get_val('excluUseAr', '0')
        try:
            area_float = float(area_raw)
            area_text = f"{area_float}㎡ (약 {area_float / 3.3058:.1f}평)"
        except:
            area_text = f"{area_raw}㎡ (약 {float(area_raw) / 3.3058:.1f}평)"

        dealing_type = get_val('dealingGbn', '거래') # 거래유형 직거래 or 중개거래 
        estate_agent_location = get_val('estateAgentSggNm', '정보없음') # 중개사소재지
        
        cdeal_type = get_val('cdealType', '') # 해제여부 O or None

        record_str = (
            f"오피스텔 매매 거래입니다. "
            f"거래일자는 {deal_date}입니다. "
            f"{dong} (지번: {jibun})에 위치한 "
            f"'{offi_name}' 오피스텔 {floor_str} 매물이 "
            f"{deal_amount}에 {dealing_type}되었습니다. "
            f"전용면적은 {area_text}이며, 건축년도는 {build_year}년입니다. "
        )

        if dealing_type == '중개거래':
            record_str += f"중개사소재지는 {estate_agent_location}입니다. "

        if cdeal_type == 'O':
            cdeal_day = get_val('cdealDay','') # 해제사유발생일 ex 25.12.12 -> 2025년 12월 12일
            yy, mm, dd = cdeal_day.split('.')
            cdeal_date = f"20{yy}년 {mm.zfill(2)}월 {dd.zfill(2)}일"
            record_str += f" 이 거래는 {cdeal_date}에 해제되었습니다."

        sler_gbn = get_val('slerGbn', '정보없음')
        buyer_gbn = get_val('buyerGbn', '정보없음')
        record_str += f"매도자 구분은 {sler_gbn}, 매수자 구분은 {buyer_gbn}입니다."

        # record_str = (
        #     f"지역코드: {str(record.get('sggCd',''))}\n"
        #     f"시군구: {str(record.get('sggNm',''))}\n"
        #     f"법정동명: {str(record.get('umdNm',''))}\n"
        #     f"지번: {str(record.get('jibun',''))}\n"
        #     f"단지명: {str(record.get('offiNm', ''))}\n"
        #     f"전용면적(㎡): {str(record.get('excluUseAr', ''))}\n"
        #     f"계약년도: {str(record.get('dealYear',''))}\n"
        #     f"계약월: {str(record.get('dealMonth',''))}\n"
        #     f"계약일: {str(record.get('dealDay',''))}\n"
        #     f"거래금액(만원): {str(record.get('dealAmount','')).replace(',', '')}\n"
        #     f"층: {str(record.get('floor', ''))}\n"
        #     f"건축년도: {str(record.get('buildYear', ''))}\n"
        #     f"해제여부: {str(record.get('cdealType',''))}\n"
        #     f"해제사유발생일: {str(record.get('cdealDay',''))}\n"
        #     f"거래유형: {str(record.get('dealingGbn',''))}\n"
        #     f"중개사소재지: {str(record.get('estateAgentSggNm',''))}\n"
        #     f"매도자: {str(record.get('slerGbn', ''))}\n"
        #     f"매수자: {str(record.get('buyerGbn', ''))}\n"
        # )

        # 위에 문장을 자연스러운 문장으로 변환 ex "2025년 11월 15일에 거래된 오피스텔 매매 실거래가 정보입니다. ..."
        # record_str = (
        #     f"{str(record.get('dealYear',''))}년 {str(record.get('dealMonth',''))}월 {str(record.get('dealDay',''))}일에 거래된 오피스텔 매매 실거래가 정보입니다.\n"
        #     f"지역코드: {str(record.get('sggCd',''))}, 시군구: {str(record.get('sggNm',''))}, 법정동명: {str(record.get('umdNm',''))}, 지번: {str(record.get('jibun',''))}.\n"
        #     f"단지명은 {str(record.get('offiNm', ''))}, 전용면적은 {str(record.get('excluUseAr', ''))}㎡입니다.\n"
        #     f"거래금액은 {str(record.get('dealAmount','')).replace(',', '')}만원이며, 층수는 {str(record.get('floor', ''))}층입니다.\n"
        #     f"건축년도는 {str(record.get('buildYear', ''))}년이며, 해제여부는 {str(record.get('cdealType',''))}, 해제사유발생일은 {str(record.get('cdealDay',''))}입니다.\n"
        #     f"거래유형은 {str(record.get('dealingGbn',''))}, 중개사소재지는 {str(record.get('estateAgentSggNm',''))}입니다.\n"
        #     f"매도자 구분은 {str(record.get('slerGbn', ''))}, 매수자 구분은 {str(record.get('buyerGbn', ''))}입니다.\n"
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

# 오피스텔 매매 실거래가 데이터를 텍스트 파일로 저장하는 함수
def save_officetel_trade_data_to_txt() -> None:
    # 날짜 설정
    now = datetime.datetime.now() # 현재 날짜 -> 형식 YYYY-MM-DD
    year = now.year 
    month = now.month
    day = now.day
    ym = f"{year}{month:02d}"
    prev_ym = f"{year}{month-1:02d}" if month > 1 else f"{year-1}12"

    filedate = f"{year}{month:02d}{day:02d}" # 파일명에 사용할 날짜 문자열 설정 -> YYYYMMDD

    officetel_data = get_all_officetel_trade_data(ym) + get_all_officetel_trade_data(prev_ym) # 이번달과 지난달 데이터 모두 수집하여 병합
    
    if not officetel_data:
        print("=== 이번달과 지난달 오피스텔 매매 거래 데이터가 모두 0건입니다. 파일 저장을 수행하지 않습니다. ===")
        return

    officetel_strings = return_officetel_string(officetel_data)

    # 중복 로직 시작
    previous_hashes = set()
    folder_path = "txts/officetel_real_estate"
    os.makedirs(folder_path, exist_ok=True)  # 폴더가 없으면 생성

    for file in glob.glob(os.path.join(folder_path, f"officetel_data_{ym}*.txt")) + glob.glob(os.path.join(folder_path, f"officetel_data_{prev_ym}*.txt")): 
        file_hashes = load_previous_hashes(file)
        previous_hashes.update(file_hashes)

    print(f"=== 이전 파일에서 {len(previous_hashes)}개의 해시 로드 완료 ===")
    # 중복 제거 후 최종 저장할 데이터 리스트
    filtered_list: list[dict] = []  

    for record in officetel_strings:
        content = record.get("content", "")
        content_hash = md5_hash(content)
        if content_hash not in previous_hashes:
            filtered_list.append(record)
    print(f"=== 중복 제거 후 최종 저장할 데이터 건수: {len(filtered_list)}건 ===")
    # 중복 로직 끝
    
    if len(filtered_list) == 0:
        print("=== 신규 데이터가 0건이므로 파일 저장을 수행하지 않습니다. ===")
        return

    # txts/officetel_real_estate/ 폴더에 저장
    filename = f"txts/officetel_real_estate/officetel_data_{filedate}.txt"
    # 텍스트 파일로 생성해서 저장
    with open(filename, "w", encoding="utf-8") as f:
        for record in filtered_list:
            f.write(json.dumps(record, ensure_ascii=False))
            f.write("\n")  # 각 기록 사이에 줄바꿈 추가
    
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
            print(f"오류 발생 {region_name} ({lawd_code}): {e}")
    
    return all_officetel_rent_data

# 병합된 딕셔너리 리스트를 문자열로 반환 함수
def return_officetel_rent_string(data: list[dict]) -> list[dict]:
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
                    return f"{eok}억원" if man == 0 else f"{eok}억 {man:}만원"
                return f"{val}만원"
            except:
                return "0원"
            
    for record in data:

        year = get_val('dealYear')
        month = get_val('dealMonth').zfill(2)
        day = get_val('dealDay').zfill(2)
        deal_date = f"{year}년 {month}월 {day}일"

        dong = get_val('umdNm')
        sgg = get_val('sggNm')
        dongf = f"{sgg} {dong}"
        jibun = get_val('jibun')
        offi_name = get_val('offiNm')
        floor = get_val('floor')
        floor_str = f"{floor}층" if floor else "층수미상"
        build_year = get_val('buildYear')

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
        area_raw = get_val('excluUseAr', '0')
        try:
            area_float = float(area_raw)
            area_text = f"{area_float}㎡ (약 {area_float / 3.3058:.1f}평)"
        except:
            area_text = f"{area_raw}㎡ (약 {float(area_raw) / 3.3058:.1f}평)"

        # --- 5. 계약 및 갱신 정보 ---
        contract_type = get_val('contractType') # 신규/갱신/공백 계약구분 
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
            
        # 갱신일 경우 종전 계약 정보 구성
        prev_contract_str = ""
        if contract_type == "갱신":
            pre_dep_fmt = fmt_money(get_val('preDeposit', '0'))
            pre_mon_fmt = fmt_money(get_val('preMonthlyRent', '0'))
                
            # 종전 월세가 있는지 확인
            try:
                pre_mon_int = int(get_val('preMonthlyRent', '0').replace(',', ''))
            except:
                pre_mon_int = 0

            if pre_mon_int > 0: # 종전 월세가 있으면 월세   
                prev_contract_str = f"종전계약보증금은 {pre_dep_fmt}, 종전계약월세 {pre_mon_fmt}"
            elif pre_dep_fmt != '0': # 종전 월세가 없으면 전세
                prev_contract_str = f"종전계약전세금은 {pre_dep_fmt}"
            else:
                prev_contract_str = "정보없음"

        record_str = (
            # f"지역코드: {str(record.get('sggCd',''))}\n"
            # f"시군구: {str(record.get('sggNm',''))}\n"
            # f"법정동명: {str(record.get('umdNm',''))}\n"
            # f"지번: {str(record.get('jibun',''))}\n"
            # f"단지명: {str(record.get('offiNm', ''))}\n"
            # f"전용면적(㎡): {str(record.get('excluUseAr', ''))}\n"
            # f"계약년도: {str(record.get('dealYear',''))}\n"
            # f"계약월: {str(record.get('dealMonth',''))}\n"
            # f"계약일: {str(record.get('dealDay',''))}\n"
            # f"보증금액(만원): {str(record.get('deposit','')).replace(',', '')}\n"
            # f"월세금액(만원): {str(record.get('monthlyRent','')).replace(',', '')}\n"
            # f"층: {str(record.get('floor', ''))}\n"
            # f"건축년도: {str(record.get('buildYear', ''))}\n"
            # f"계약기간: {str(record.get('contractTerm',''))}\n"
            # f"갱신요구권사용: {str(record.get('useRRRight',''))}\n"
            # f"종전계약보증금(만원): {str(record.get('preDeposit',''))}\n"
            # f"종전계약월세(만원): {str(record.get('preMonthlyRent',''))}\n"
            f"오피스텔 {deal_type} 거래입니다. "
            f"거래일자는 {deal_date}입니다. "
            f"{dong} (지번: {jibun})에 위치한 "
            f"'{offi_name}' 오피스텔 {floor_str} 매물이 "
            f"{price_text}으로 "
            f"{'계약되었습니다.' if contract_type != '갱신' else '갱신 계약되었습니다.'} "
            f"전용면적은 {area_text}이며, 건축년도는 {build_year}년입니다. "
            f"계약기간은 {term}입니다. "
            f"갱신요구권은 {'사용' if use_rr == '사용' else '미사용'}입니다."
        )

        if prev_contract_str and contract_type == "갱신":
            record_str += f" {prev_contract_str}입니다."
        
        
        last_data = {
            "metadata": {
                "region_code": record.get('sggCd',''),
                "enactment_date": f"{str(record.get('dealYear',''))}{str(record.get('dealMonth','')).zfill(2)}{str(record.get('dealDay','')).zfill(2)}"
            },
            "content": record_str
        }

        result_string.append(last_data)

    return result_string

# 오피스텔 전월세 실거래가 데이터를 텍스트 파일로 저장하는 함수
def save_officetel_rent_data_to_txt() -> None:
    # 날짜 설정
    now = datetime.datetime.now() # 현재 날짜 -> 형식 YYYY-MM-DD
    year = now.year
    month = now.month
    day = now.day
    ym = f"{year}{month:02d}"
    prev_ym = f"{year}{month-1:02d}" if month > 1 else f"{year-1}12"

    filedate = f"{year}{month:02d}{day:02d}" # 파일명에 사용할 날짜 문자열 설정 -> YYYYMMDD

    officetel_rent_data = get_all_officetel_rent_data(ym) + get_all_officetel_rent_data(prev_ym) # 이번달과 지난달 데이터 모두 수집하여 병합
    
    if not officetel_rent_data:
        print("=== 이번달과 지난달 오피스텔 전월세 거래 데이터가 모두 0건입니다. 파일 저장을 수행하지 않습니다. ===")
        return

    officetel_rent_strings = return_officetel_rent_string(officetel_rent_data)

    # 중복 로직 시작
    previous_hashes = set()
    folder_path = "txts/officetel_real_estate"
    os.makedirs(folder_path, exist_ok=True)  # 폴더가 없으면 생성

    for file in glob.glob(os.path.join(folder_path, f"officetel_rent_data_{ym}*.txt")) + glob.glob(os.path.join(folder_path, f"officetel_rent_data_{prev_ym}*.txt")): 
        file_hashes = load_previous_hashes(file)
        previous_hashes.update(file_hashes)

    print(f"=== 이전 파일에서 {len(previous_hashes)}개의 해시 로드 완료 ===")
    # 중복 제거 후 최종 저장할 데이터 리스트
    filtered_list: list[dict] = []

    for record in officetel_rent_strings:
        content = record.get("content", "")
        content_hash = md5_hash(content)
        if content_hash not in previous_hashes:
            filtered_list.append(record)
    print(f"=== 중복 제거 후 최종 저장할 데이터 건수: {len(filtered_list)}건 ===")
    # 중복 로직 끝
    
    if len(filtered_list) == 0:
        print("=== 신규 데이터가 0건이므로 파일 저장을 수행하지 않습니다. ===")
        return

    filename = f"txts/officetel_real_estate/officetel_rent_data_{filedate}.txt"
    # 텍스트 파일로 생성해서 저장
    with open(filename, "w", encoding="utf-8") as f:
        for record in filtered_list:
            f.write(json.dumps(record, ensure_ascii=False))
            f.write("\n")  # 각 기록 사이에 줄바꿈 추가
    
    print(f"오피스텔 전월세 거래 데이터가 '{filename}' 파일로 저장되었습니다.")


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


if __name__ == "__main__":
    # print("=== 오피스텔 매매 실거래가 데이터 수집 테스트 ===\n")
    # print("\n=== 오피스텔 전월세 실거래가 데이터 수집 테스트 ===\n")
    save_officetel_trade_data_to_txt()
    save_officetel_rent_data_to_txt()