# 아파트 매매, 전월세 실거래가 데이터 수집 

from langchain_core.documents import Document
from dataPortal.apt_list import apt_sub
import datetime #  main에서 날짜 설정을 위해 임포트
import pandas as pd
import schedule
import time
import json
import os
import glob

# 아파트 매매 실거래가 txt 파일로 저장하는 함수 구현
def save_apt_data_to_txt() -> None:

    now = datetime.datetime.now()
    year = now.year
    month = now.month
    day = now.day
    ym = f"{year}{month:02d}"
    total_df = apt_sub.get_all_apt_trade_data(ym)
    if total_df.empty:
        print(f"=== {ym} 기간에 조회된 실거래가 데이터가 전혀 없습니다. ===")
        return
    
    text_strings: list[dict] = apt_sub.return_apt_string(total_df)

    # 중복 로직 추가 
    previous_hashes = set()
    folder_path = "txts/apt_real_estate"
    os.makedirs(folder_path, exist_ok=True)  # 폴더가 없으면 생성

    for file in glob.glob(os.path.join(folder_path, f"apt_data_{ym}*.txt")): # 경로는 
        file_hashes = load_previous_hashes(file)
        previous_hashes.update(file_hashes)

    print(f"=== 이전 파일에서 {len(previous_hashes)}개의 중복 해시 로드 완료 ===")
    filtered_list: list[dict] = []
    for rent in text_strings:
        content = rent.get("content", "")
        content_hash = md5_hash(content)
        if content_hash not in previous_hashes:
            filtered_list.append(rent)
    print(f"=== 중복 제거 후 최종 저장할 데이터 건수: {len(filtered_list)}건 ===")
    ############# 중복 로직 끝 ######################

    filedate = f"{year}{month:02d}{day:02d}" # 파일명에 사용할 날짜 문자열 설정 -> YYYYMMDD
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
    date = f"{year}{month:02d}{now.day:02d}" # 파일명에 사용할 날짜 문자열 설정 -> YYYYMMDD
    rent_data = apt_sub.get_all_apt_rent_data(ym)
    rent_strings = apt_sub.return_apt_rent_string(rent_data)

    # 중복 로직 추가 
    previous_hashes = set()
    folder_path = "txts/apt_real_estate"
    os.makedirs(folder_path, exist_ok=True)  # 폴더가 없으면 생성

    for file in glob.glob(os.path.join(folder_path, f"apt_rent_data_{ym}*.txt")): 
        file_hashes = load_previous_hashes(file)
        previous_hashes.update(file_hashes)

    print(f"=== 이전 파일에서 {len(previous_hashes)}개의 중복 해시 로드 완료 ===")
    
    filtered_list: list[dict] = []
    for rent in rent_strings:
        content = rent.get("content", "")
        content_hash = md5_hash(content)
        if content_hash not in previous_hashes:
            filtered_list.append(rent)
    print(f"=== 중복 제거 후 최종 저장할 데이터 건수: {len(filtered_list)}건 ===")
    ############# 중복 로직 끝 ######################
    
    # 파일 저장
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