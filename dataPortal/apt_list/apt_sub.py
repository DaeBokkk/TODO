import os
import sys
import time
from datakart import Datagokr
# 프로젝트 경로 설정 
# api 폴더의 상위 폴더를 루트로 간주
# region 모듈 임포트 위해 필요
ROOT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.insert(0, ROOT_PATH)
from dataPortal import region
import pandas as pd
import requests
import xmltodict
import dotenv

# 초기화 
# .env 파일에서 환경 변수 로드 (.env 파일에 DATAGO_KEY 설정 필요) 
# ex) DATAGO_KEY = "API KEY값" 
dotenv.load_dotenv()  # .env 파일에서 환경 변수 로드
DATAGO_KEY = os.getenv("DATAGO_KEY")
datago = Datagokr(DATAGO_KEY)

# 단일 지역 데이터 조회 함수 
def apt_trade_data(lawd_cd: str, deal_ymd: str) -> pd.DataFrame:
    """아파트 실거래가 데이터 조회 함수 (DataFrame 반환)"""
    """ConnectionError 발생 시 3회 재시도 (Exponential Backoff)"""
    max_retries = 3 # 최대 재시도 횟수
    base_delay_seconds = 1 # 기본 지연 시간 (초)
    for attempt in range(max_retries):
        try:
            res = datago.apt_trade(lawd_cd, deal_ymd)
    
            if not res:
                return pd.DataFrame() # 데이터가 없으면 빈 DataFrame 반환
        
            df = pd.DataFrame(res)

            # datakart가 데이터가 없을 때 컬럼명 리스트(List[str])를 반환하는 경우 방지
            # res의 첫 번째 항목이 딕셔너리(dict)인지 반드시 확인.
            if not isinstance(res[0], dict):
                # res[0]이 'aptNm' 같은 문자열이면, 데이터가 없는 것으로 간주
                print(f"    -> [정보] 데이터 없음 (API가 컬럼명 리스트 반환).")
                return pd.DataFrame()

            # 3. 딕셔너리 리스트이므로 DataFrame 생성 (이제 안전함)
            df = pd.DataFrame(res)
    
            # 4. 컬럼이 0개인 DataFrame (e.g., [{}]) 방지
            if len(df.columns) == 0:
                return pd.DataFrame()

            # 5. 모든 값이 NaN인 행 제거
            df.dropna(how='all', inplace=True)

            # 6. 핵심 컬럼 기준으로 NaN 행 제거
            KEY_COLUMNS = ['aptNm', 'dealAmount', 'sggCd'] 
            valid_key_columns = [col for col in KEY_COLUMNS if col in df.columns]

            if valid_key_columns:
                df.dropna(subset=valid_key_columns, how='all', inplace=True)
            else:
                print(f"경고: DataFrame에 핵심 컬럼({KEY_COLUMNS})이 없습니다. API 응답 필드명을 확인하세요.")
            # 7. 최종 DataFrame 반환
            return df

        except requests.exceptions.ConnectionError as e:
            print(f"    -> [경고] Connection Error 발생: {e}")
            if attempt < max_retries - 1:
                delay = base_delay_seconds * (2 ** attempt)
                print(f"    -> {delay}초 후 재시도... ({attempt + 1}/{max_retries})")
                time.sleep(delay)
            else:
                print(f"    -> [실패] 최종 재시도 실패.")
        
        # 5. 기타 예외 처리
        except Exception as e:
            print(f"    -> [오류] 알 수 없는 오류 발생: {e}. 이 지역을 건너뜁니다.")
            break # 재시도하지 않고 이 지역은 포기

    # 6. 모든 재시도 실패 시 빈 DataFrame 반환
    return pd.DataFrame()

# 모든 지역 데이터 수집 및 병합 함수 
def get_all_apt_trade_data(ym: str) -> pd.DataFrame:
    """
    모든 '시/군/구' 지역의 실거래가 데이터를 수집하여 
    하나의 거대한 DataFrame으로 병합하여 반환합니다.
    """
    all_dataframes = []
    
    print(f"=== {ym} 전국 아파트 실거래가 데이터 수집 시작 ===")
    
    region_dict = region.get_all_sgg_code_dict()
    total_regions = len(region_dict)
    
    for i, (region_name, lawd_cd) in enumerate(region_dict.items()):
        
        print(f" - [{i+1}/{total_regions}] {region_name} ({lawd_cd}) 데이터 수집 중...")
        
        df_apt = apt_trade_data(lawd_cd, ym) # 3번 함수 호출
        
        if not df_apt.empty:
            all_dataframes.append(df_apt)
        else:
            print(f"    -> {region_name} 지역은 법정동코드({lawd_cd}), {ym}월 거래 내역이 없습니다.")

    if not all_dataframes:
        print(f"=== {ym} 기간에 조회된 실거래가 데이터가 전혀 없습니다. ===")
        return pd.DataFrame() # 빈 DataFrame 반환

    # 모든 DataFrame 병합
    print("\n=== 모든 지역 데이터 병합 중... ===")
    total_df = pd.concat(all_dataframes, ignore_index=True)
    print(f"병합 완료. {ym} 전국 총 실거래가 데이터: {len(total_df)}건")
    
    return total_df

# 병합된 DataFrame을 문자열 리스트로 변환 함수
def return_apt_string(df: pd.DataFrame) -> list[dict]:
    """
    병합된 DataFrame을 받아서, RAG 시스템에 적합한 
    [{metadata: ..., content: ...}, ...] 형태의 리스트로 반환합니다.
    """
    
    # 금액을 '억/만원' 단위로 변환하는 헬퍼 함수
    def _format_money(amount_10k):
        try:
            amount = int(str(amount_10k).replace(',', ''))
            if amount == 0: return "0원"
            eok = amount // 10000
            man = amount % 10000
            
            result = []
            if eok > 0: result.append(f"{eok}억")
            if man > 0: 
                # 4억 5000 등 가독성 좋게
                result.append(f"{man:,}천" if man % 1000 == 0 and man != 0 else f"{man:,}") 
            
            if man > 0:
                return f"{eok}억 {man:}만원" if eok > 0 else f"{man:}만원"
            else:
                return f"{eok}억원"
        except:
            return "0원"

    result_list = []

    # DataFrame이 비어있으면 빈 리스트 반환
    if df.empty:
        print("입력된 DataFrame이 비어있습니다.")
        return []

    print(f"\n=== {len(df)}건의 매매 데이터를 RAG 포맷으로 변환 시작... ===")

    # DataFrame의 각 행을 순회 (iterrows는 속도가 느리지만, 로직이 복잡할 때 안전함)
    for idx, row in df.iterrows():
        try:
            # --- 1. 기본 정보 추출 ---
            # 날짜 패딩 처리 (1월 -> 01)
            year = str(row['dealYear'])
            month = str(row['dealMonth']).zfill(2) 
            day = str(row['dealDay']).zfill(2)
            deal_date_str = f"{year}년 {int(month)}월 {int(day)}일"
            
            # 아파트 및 위치 정보
            apt_name = row['aptNm']
            jibun = row['jibun']
            floor = row['floor']
            floor_str = f"{floor}층" if pd.notna(floor) else "층수 미상"
            
            # 전용면적 및 건축년도
            area = row['excluUseAr']
            build_year = row['buildYear']

            # 동 정보 (선택)
            apt_dong_info = ""
            if pd.notna(row.get('aptDong')) and str(row['aptDong']).strip():
                apt_dong_info = f" {row['aptDong']}동"

            # --- 2. 거래 금액 및 상태 ---
            # 금액 포맷팅
            raw_amount = row['dealAmount']
            deal_amount_str = _format_money(raw_amount)

            # 거래 유형 (직거래/중개거래)
            estate_agent_info = ""
            if pd.isna(row.get('estateAgentSggNm')) or str(row.get('estateAgentSggNm')).strip() == '':
                 estate_agent_info = f"{row['umdNm']} 직거래"
            else:
                 estate_agent_info = f"중개사 소재지: {row['estateAgentSggNm']} 법정동 {row['umdNm']}"

            # 해제 여부 확인
            deal_status = f"거래금액 {deal_amount_str}에 {row['dealingGbn']}되었습니다."
            if pd.notna(row.get('cdealType')) and row['cdealType'] == 'O':
                 cdeal_day = row.get('cdealDay', '날짜미상')
                 deal_status = f"거래금액 {deal_amount_str}에 {row['dealingGbn']}되었으나, {cdeal_day}부로 해제되었습니다."

            # 토지임대부 여부
            land_lease_info = ""
            if pd.notna(row.get('landLeaseholdGbn')) and str(row['landLeaseholdGbn']) == 'Y':
                 land_lease_info = " (토지임대부)"

            # --- 3. 최종 문장 조합 (Content) ---
            # 문맥: [날짜], [중개정보] [아파트] [동] [층]이 [상태]. [건물정보].
            text_chunk = (
                f"{deal_date_str}, '{estate_agent_info}' (지번: {jibun})에 위치한 "
                f"'{apt_name}' 아파트{apt_dong_info} {floor_str} 매물이 "
                f"{deal_status} "
                f"이 단지는 {build_year}년에 준공되었으며, 전용면적은 {area}㎡입니다.{land_lease_info}"
            )
            
            # 공백 정리 (더블 스페이스 제거)
            text_chunk = " ".join(text_chunk.split())

            # --- 4. 데이터 구조화 (Rent 로직과 동일) ---
            last_data = {
                "metadata": {
                    "region_code": str(row['sggCd']),
                    # YYYYMMDD 포맷으로 변환하여 메타데이터 저장
                    "enactment_date": f"{year}{month}{day}"
                },
                "content": text_chunk
            }

            result_list.append(last_data)

        except Exception as e:
            print(f"    -> [경고] 변환 중 오류 발생: {e} | 아파트명: {row.get('aptNm', 'Unknown')}")
            continue
    
    print(f"변환 완료. (성공: {len(result_list)}건)")
    return result_list

# 아파트 전원세 조회 함수 (단일 지역, 단일 월)
def apt_trade_rent(lawd_code: str, deal_ym: str, n_rows: int = 9999) -> list[dict]:
    # https://www.data.go.kr/data/15126469/openapi.do
    def _api_call(lawd_code: str, deal_ym: str, n_rows: int, page: int) -> dict:
        url = "https://apis.data.go.kr/1613000/RTMSDataSvcAptRent/getRTMSDataSvcAptRent"
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

# 모든 지역 전원세 데이터 수집 및 병합 함수
def get_all_apt_rent_data(ym: str) -> list[dict]:
    """
    모든 '시/군/구' 지역의 아파트 전원세 데이터를 수집하여 
    하나의 거대한 리스트로 병합하여 반환합니다.
    """
    all_rent_data = []
    
    print(f"=== {ym} 전국 아파트 전원세 데이터 수집 시작 ===")
    
    region_dict = region.get_all_sgg_code_dict()
    total_regions = len(region_dict)
    
    for i, (region_name, lawd_cd) in enumerate(region_dict.items()):
        
        print(f" - [{i+1}/{total_regions}] {region_name} ({lawd_cd}) 데이터 수집 중...")
        
        rent_data = apt_trade_rent(lawd_cd, ym) 
        
        if rent_data:
            all_rent_data.extend(rent_data)
        else:
            print(f"    -> {region_name} 지역은 법정동코드({lawd_cd}), {ym}월 전원세 거래 내역이 없습니다.")

    print(f"\n=== 모든 지역 데이터 병합 완료. {ym} 전국 총 전원세 데이터: {len(all_rent_data)}건 ===")
    
    return all_rent_data

# 전월세 데이터 리스트를 문자열 리스트로 변환 함수
def return_apt_rent_string(data: list[dict]) -> list[dict]:
    """
    전월세 데이터 리스트를 받아, RAG 및 사람이 읽기 좋은 자연어 문장 리스트로 반환합니다.
    """
    
    # 금액을 '억/만원' 단위로 변환하는 헬퍼 함수
    def _format_money(amount_10k):
        try:
            amount = int(amount_10k)
            if amount == 0: return "0원"
            eok = amount // 10000
            man = amount % 10000
            
            result = []
            if eok > 0: result.append(f"{eok}억")
            if man > 0: result.append(f"{man:}천" if man % 1000 == 0 and man != 0 else f"{man:}") 
            # 간단하게 '4억 5,000' 식으로 표기하거나, 뒤에 '만원'을 붙임
            if man > 0:
                return f"{eok}억 {man:}만원" if eok > 0 else f"{man:}만원"
            else:
                return f"{eok}억원"
        except:
            return "0원"

    result_list = []

    for item in data:
        try:
            # --- 1. 기본 정보 ---
            deal_date = f"{item.get('dealYear')}년 {item.get('dealMonth')}월 {item.get('dealDay')}일"
            dong = item.get('umdNm', '')
            apt_name = item.get('aptNm', '')
            floor = item.get('floor', '')
            floor_str = f"{floor}층" if floor else "층수 미상"
            
            area = item.get('excluUseAr', '')
            build_year = item.get('buildYear', '')

            # --- 2. 금액 정보 (None 처리 및 정수 변환) ---
            # API 값(단위: 만원)을 가져옴. 값이 없으면 '0'으로 처리
            deposit_val = item.get('deposit') or '0' 
            monthly_val = item.get('monthlyRent') or '0'
            
            deposit_int = int(str(deposit_val).replace(',', '').strip())
            monthly_int = int(str(monthly_val).replace(',', '').strip())

            # --- 3. 거래 형태 판단 ---
            deal_type = ""
            price_text = ""
            
            if monthly_int > 0:
                deal_type = "월세" # 또는 반전세
                price_text = f"보증금 {_format_money(deposit_int)}에 월세 {_format_money(monthly_int)}"
            else:
                deal_type = "전세"
                price_text = f"전세금 {_format_money(deposit_int)}"

            # --- 4. 계약 정보 (신규/갱신/기간) ---
            contract_type = item.get('contractType') # 신규, 갱신, 또는 None
            contract_type_str = "" 
            
            if contract_type:
                contract_type = contract_type.strip()
                contract_type_str = f" '{contract_type}'로" # 예: '신규'로, '갱신'으로
            else:
                contract_type_str = "" # 정보가 없으면 생략

            # 계약 기간 처리 (24.02~26.02 형태)
            term_raw = str(item.get('contractTerm') or '')
            term_str = ""
            if '~' in term_raw:
                # 24.02 -> 2024년 2월 형태로 변환 로직 필요하면 추가, 여기선 그대로 사용하거나 간단 변환
                term_str = f" 계약 기간은 {term_raw}입니다."
            
            # --- 5. 갱신인 경우 종전 계약 정보 (Optional) ---
            prev_contract_str = ""
            if contract_type == "갱신":
                # 종전 보증금/월세 확인
                pre_deposit = item.get('preDeposit') or '0'
                pre_monthly = item.get('preMonthlyRent') or '0'
                
                pre_dep_int = int(str(pre_deposit).replace(',', ''))
                pre_mon_int = int(str(pre_monthly).replace(',', ''))
                
                if pre_dep_int > 0 or pre_mon_int > 0:
                    prev_text = ""
                    if pre_mon_int > 0:
                        prev_text = f"보증금 {_format_money(pre_dep_int)}/월세 {_format_money(pre_mon_int)}"
                    else:
                        prev_text = f"전세금 {_format_money(pre_dep_int)}"
                    
                    # 증감 확인 (단순 비교)
                    diff = deposit_int - pre_dep_int
                    diff_str = ""
                    if diff > 0: diff_str = f"({_format_money(diff)} 인상)"
                    elif diff < 0: diff_str = f"({_format_money(abs(diff))} 인하)"
                    
                    prev_contract_str = f" (종전 계약: {prev_text}{diff_str})"

            # 갱신요구권 사용 여부
            rr_right = item.get('useRRRight')
            rr_str = " (계약갱신요구권 사용)" if rr_right == '사용' else ""

            # --- 6. 최종 문장 조합 ---
            # 문맥: [날짜], [위치] [아파트] [층]이 [가격]으로 [전/월세] [신규/갱신] 계약되었습니다. [기간]. [종전정보]. [건물정보].
            
            text_chunk = (
                f"{deal_date}, 법정동 '{dong}'에 위치한 '{apt_name}' 아파트 {floor_str} 매물이 "
                f"{price_text}으로 {deal_type}{contract_type_str} 계약되었습니다."
                f"{term_str}{prev_contract_str}{rr_str} "
                f"이 단지는 {build_year}년에 준공되었으며, 해당 세대의 전용면적은 {area}㎡입니다."
            )
            # 공백 정리 (더블 스페이스 제거)
            text_chunk = text_chunk.replace("  ", " ")
            
            last_data = {
                "metadata": {
                    "region_code": item.get('sggCd',''),
                    "enactment_date": f"{item.get('dealYear','')}{item.get('dealMonth','')}{item.get('dealDay','')}"
                },
                "content": text_chunk
            }

            result_list.append(last_data)


        except Exception as e:
            print(f"    -> [경고] 변환 중 오류 발생: {e} | 데이터: {item.get('aptNm', 'Unknown')}")
            continue

    return result_list

