# 아파트 매매, 전월세 실거래가 데이터 수집 

from dataPortal.apt_list import apt_sub
import datetime #  main에서 날짜 설정을 위해 임포트
import json
import os
import glob
import pandas as pd

# 아파트 매매 실거래가 txt 파일로 저장하는 함수 구현
def save_apt_data_to_txt() -> None:

    now = datetime.datetime.now()
    year = now.year
    month = now.month
    day = now.day
    ym = f"{year}{month:02d}"
    prev_ym = f"{year}{month-1:02d}" if month > 1 else f"{year-1}12"

    # df1 = apt_sub.get_all_apt_trade_data(ym)
    # df2 = apt_sub.get_all_apt_trade_data(prev_ym)

    # total_df = pd.concat([df1, df2], ignore_index=True)

    # total_df = apt_sub.get_all_apt_trade_data("202501") + apt_sub.get_all_apt_trade_data("202502") + apt_sub.get_all_apt_trade_data("202503") + apt_sub.get_all_apt_trade_data("202504") + apt_sub.get_all_apt_trade_data("202505") + apt_sub.get_all_apt_trade_data("202506") + apt_sub.get_all_apt_trade_data("202507") + apt_sub.get_all_apt_trade_data("202508") + apt_sub.get_all_apt_trade_data("202509") + apt_sub.get_all_apt_trade_data("202510") + apt_sub.get_all_apt_trade_data("202511") + apt_sub.get_all_apt_trade_data("202512")
    total_df = pd.concat([pd.DataFrame(apt_sub.get_all_apt_trade_data("202401")), pd.DataFrame(apt_sub.get_all_apt_trade_data("202402")), pd.DataFrame(apt_sub.get_all_apt_trade_data("202403")), pd.DataFrame(apt_sub.get_all_apt_trade_data("202404")), pd.DataFrame(apt_sub.get_all_apt_trade_data("202405")), pd.DataFrame(apt_sub.get_all_apt_trade_data("202406")), pd.DataFrame(apt_sub.get_all_apt_trade_data("202407")), pd.DataFrame(apt_sub.get_all_apt_trade_data("202408")), pd.DataFrame(apt_sub.get_all_apt_trade_data("202409")), pd.DataFrame(apt_sub.get_all_apt_trade_data("202410")), pd.DataFrame(apt_sub.get_all_apt_trade_data("202411")), pd.DataFrame(apt_sub.get_all_apt_trade_data("202412"))], ignore_index=True)
    print(f"=== 이번달과 지난달에 조회된 아파트 매매 거래 데이터 총 {len(total_df)}건 수집됨. ===")

    if total_df.empty:
        print(f"=== {ym}와 {prev_ym} 기간에 조회된 실거래가 데이터가 전혀 없습니다. ===")
        return
    
    text_strings: list[dict] = apt_sub.return_apt_string(total_df)

    # 중복 로직 추가 
    previous_hashes = set()
    folder_path = "txts/apt_real_estate"
    os.makedirs(folder_path, exist_ok=True)  # 폴더가 없으면 생성

    for file in glob.glob(os.path.join(folder_path, f"apt_data_{ym}*.txt")) + glob.glob(os.path.join(folder_path, f"apt_data_{prev_ym}*.txt")):  # 이번달과 지난달 파일 패턴과 일치하는 기존 파일들에서 해시 로드
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
    filedate = "20240101" # 테스트용으로 고정된 날짜 사용 (실제 운영 시에는 위의 동적 날짜 사용)
    filename = f"txts/apt_real_estate/apt_data_{filedate}.txt" # 파일명 설정 -> real_estate/apt_documents_YYYYMMDD.txt
    with open(filename, 'w', encoding='utf-8') as f:
        for text in (filtered_list):
            f.write(json.dumps(text, ensure_ascii=False))
            f.write("\n")  # 각 문서 구분을 위한 빈 줄 추가
    print(f"텍스트 파일로 저장 완료: {filename}")

# 전원세 문자열 리스트를 텍스트 파일로 저장하는 함수 (스케줄링 가능)
def save_apt_rent_data_to_txt():
    """전원세 문자열 리스트를 텍스트 파일로 저장하는 함수"""
    now = datetime.datetime.now()
    year = now.year
    month = now.month
    ym = f"{year}{month:02d}"
    prev_ym = f"{year}{month-1:02d}" if month > 1 else f"{year-1}12"
    date = f"{year}{month:02d}{now.day:02d}" # 파일명에 사용할 날짜 문자열 설정 -> YYYYMMDD
    # rent_data = apt_sub.get_all_apt_rent_data(ym) + apt_sub.get_all_apt_rent_data(prev_ym) # 이번달과 지난달 데이터 모두 수집하여 병합
    rent_data = apt_sub.get_all_apt_rent_data("202401") + apt_sub.get_all_apt_rent_data("202402") + apt_sub.get_all_apt_rent_data("202403") + apt_sub.get_all_apt_rent_data("202404") + apt_sub.get_all_apt_rent_data("202405") + apt_sub.get_all_apt_rent_data("202406") + apt_sub.get_all_apt_rent_data("202407") + apt_sub.get_all_apt_rent_data("202408") + apt_sub.get_all_apt_rent_data("202409") + apt_sub.get_all_apt_rent_data("202410") + apt_sub.get_all_apt_rent_data("202411") + apt_sub.get_all_apt_rent_data("202412")
    print(f"=== 이번달과 지난달에 조회된 아파트 전월세 거래 데이터 총 {len(rent_data)}건 수집됨. ===")

    if not rent_data:
        print(f"=== {ym}와 {prev_ym} 기간에 조회된 전월세 데이터가 전혀 없습니다. ===")
        return
    
    rent_strings = apt_sub.return_apt_rent_string(rent_data)

    # 중복 로직 추가 
    previous_hashes = set()
    folder_path = "txts/apt_real_estate"
    os.makedirs(folder_path, exist_ok=True)  # 폴더가 없으면 생성

    for file in glob.glob(os.path.join(folder_path, f"apt_rent_data_{ym}*.txt")) + glob.glob(os.path.join(folder_path, f"apt_rent_data_{prev_ym}*.txt")):  # 이번달과 지난달 파일 패턴과 일치하는 기존 파일들에서 해시 로드
        file_hashes = load_previous_hashes(file)
        previous_hashes.update(file_hashes)

    print(f"=== 이전 파일에서 {len(previous_hashes)}개의 해시 로드 완료 ===")
    
    filtered_list: list[dict] = []
    for rent in rent_strings:
        content = rent.get("content", "") # content 필드에서 문자열 추출
        content_hash = md5_hash(content) # 문자열의 MD5 해시값 계산
        if content_hash not in previous_hashes: # 이전 해시와 비교하여 중복 여부 판단
            filtered_list.append(rent)
    print(f"=== 중복 제거 후 최종 저장할 데이터 건수: {len(filtered_list)}건 ===")
    ############# 중복 로직 끝 ######################

    if len(filtered_list) == 0:
        print("=== 신규 데이터가 0건이므로 파일 저장을 수행하지 않습니다. ===")
        return
    
    # 파일 저장
    date = "20240101" # 테스트용으로 고정된 날짜 사용 (실제 운영 시에는 동적 날짜 사용)
    filename = f"txts/apt_real_estate/apt_rent_data_{date}.txt"
    with open(filename, 'w', encoding='utf-8') as f:
        for rent in (filtered_list):
            f.write(json.dumps(rent, ensure_ascii=False))
            f.write("\n")  # 각 문서 구분을 위한 빈 줄 추가

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
    
    # 방법 1 : Document 리스트 생성 및 개수 출력

    # docs = apt_api_documents_list()
    # print(f"\n총 생성된 Document 객체 수: {len(docs)}")
    # # 생성된 Document 객체 중 첫 2개 출력 (확인용)
    # for i, doc in enumerate(docs[:2]):
    #     print(f"\n--- Document {i+1} ---")
    #     print("Page Content:\n", doc.page_content)
    #     print("Metadata:\n", doc.metadata)

    # 방법 2 : Document 텍스트 파일로 저장 스케줄링
    save_apt_data_to_txt()
    save_apt_rent_data_to_txt()