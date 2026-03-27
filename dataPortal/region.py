# 법정동 코드 수집 모듈
from datakart import Datagokr
import pandas as pd
import os
from dotenv import load_dotenv

# 환경변수 로드
load_dotenv()

# [추가된 부분] ---------------------------------------------------------
# 현재 파일(region.py)의 위치를 기준으로 프로젝트 루트의 .env 찾기
# 예: dataPortal/region.py -> 상위(루트)/.env
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(current_dir) # dataPortal의 상위 폴더
env_path = os.path.join(root_dir, "dataPortal/.env")

# .env 파일 로드
load_dotenv(dotenv_path=env_path)

# --- 1. 초기화 ---
DATAGO_KEY = os.getenv("DATAGO_KEY")
datago = Datagokr(DATAGO_KEY)

# --- 2. 모든 지역 리스트 ---
# ALL_REGIONS = [
#     "서울특별시", "경기도", "인천광역시", "부산광역시", "대구광역시", 
#     "광주광역시", "대전광역시", "울산광역시", "세종특별자치시", 
#     "강원특별자치도", "충청북도", "충청남도", "전북특별자치도", 
#     "전라남도", "경상북도", "경상남도", "제주특별자치도"
# ]

ALL_REGIONS = [
    "경기도"
]

# 지역 별 '시/군/구' 코드 조회 함수
def get_sgg_dict_for_region(region: str) -> dict:
    """
    단일 지역의 '시/군/구' 레벨 법정동 코드를 조회하고 
    { '지역명': '코드' } 딕셔너리로 반환합니다.
    """
    try:
        # 'locatadd_nm' (지역명) 컬럼을 다시 포함
        res = datago.lawd_code(region=region)
        df = pd.DataFrame(res).filter(['sido_cd','sgg_cd','umd_cd','ri_cd', 'locatadd_nm'])

        # '시/군/구' 레벨 필터링 (모든 지역 공통)
        df_filtered = df[
            (df['sgg_cd'] != '000') & 
            (df['umd_cd'] == '000') &
            (df['ri_cd'] == '00')
        ].copy() # SettingWithCopyWarning 방지를 위해 .copy() 추가
        
        # 5자리 코드 생성
        df_filtered['sido_sgg_cd'] = df_filtered['sido_cd'] + df_filtered['sgg_cd']
        
        # 'locatadd_nm'을 key로, 'sido_sgg_cd'를 value로 하는 딕셔너리 생성
        return dict(zip(df_filtered['locatadd_nm'], df_filtered['sido_sgg_cd']))

    except Exception as e:
        print(f"Error processing {region}: {e}")
        return {}

# 전체 지역의 '시/군/구' 코드 딕셔너리 조회 함수
def get_all_sgg_code_dict() -> dict:
    """전국 17개 시/도의 '시/군/구' 코드 딕셔너리를 모두 병합하여 반환합니다."""
    
    print("전국 '시/군/구' 법정동 코드 딕셔너리 수집 시작...")
    
    all_region_dict = {}
    for region in ALL_REGIONS:
        print(f" - {region} 수집 중...")
        # 각 지역의 딕셔너리를 가져와서
        region_dict = get_sgg_dict_for_region(region)
        # 전체 딕셔너리에 업데이트 (병합)
        all_region_dict.update(region_dict)
    
    print("전국 '시/군/구' 법정동 코드 딕셔너리 수집 완료.")
    return all_region_dict


# 법정동 코드 딕셔너리 정보 출력 함수
def print_lawd_code_dict_info(region_dict: dict):
    """법정동 코드 딕셔너리의 '항목 개수'와 '메모리 크기'를 출력합니다."""
    
    # 딕셔너리의 '항목 개수(key의 개수)'
    item_count = len(region_dict)
    
    # 딕셔너리 객체가 차지하는 '메모리 크기(bytes)'
    memory_size = region_dict.__sizeof__()
    
    print(f"법정동 코드 총 항목 개수 (시/군/구 레벨): {item_count} 개")
    print(f"딕셔너리 메모리 크기: {memory_size} bytes")

# 읍면동, 리까지 포함한 전체 법정동 코드 조회 함수 (참고용)
def get_full_lawd_code_dict() -> dict:
    """
    읍/면/동, 리까지 포함한 전체 법정동 코드를 조회하고 
    { '지역명': '코드' } 딕셔너리로 반환합니다.
    """
    try:
        res = datago.lawd_code()
        df = pd.DataFrame(res).filter(['region_cd', 'locatadd_nm', 'sgg_cd', 'umd_cd', 'ri_cd'])
        
        # 'locatadd_nm'을 key로, 'full_lawd_cd'를 value로 하는 딕셔너리 생성
        return dict(zip(df['locatadd_nm'], df['region_cd']))

    except Exception as e:
        print(f"Error processing full lawd codes: {e}")
        return {}
    

# 출력 함수
def print_full_lawd_code_dict_info(region_dict: dict):
    # 지역명 딕셔너리 출력
    for k, v in list(region_dict.items()):
        print(f"'{k}': '{v}'")
    # 총 항목 개수 및 메모리 크기 출력
    item_count = len(region_dict)
    print(f"총 갯수: {item_count} 개")

# --- 스크립트 실행 ---
# if __name__ == "__main__":
    
#     print("=== 지역 코드 딕셔너리 수집 테스트 ===\n")
#     # 엑셀 파일로 저장
#     region_dict = get_all_sgg_code_dict()
#     df_region = pd.DataFrame(list(region_dict.items()), columns=['지역명', '법정동코드'])
#     # 엑셀 파일로 저장
#     df_region.to_excel("full_lawd_code_dict.xlsx", index=False)



    # 전국 '시/군/구' 코드 딕셔너리 생성
    # total_region_dict = get_all_sgg_code_dict()
    
    # # 정보 출력
    # print_lawd_code_dict_info(total_region_dict)

    # print(total_region_dict)
    # (참고) 수집된 딕셔너리 샘플 확인
    # print("\n--- 수집된 딕셔너리 샘플 ---")
    # count = 0
    # for k, v in total_region_dict.items():
    #     if k.startswith("경기도"): # 경기도 샘플 확인
    #         print(f"'{k}': '{v}'")
    #         count += 1
    #     if count >= 5:
    #         break