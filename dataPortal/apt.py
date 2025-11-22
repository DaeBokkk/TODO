# 아파트 매매, 전월세 실거래가 데이터 수집 

from langchain_core.documents import Document
from apt import apt_sub
import datetime #  main에서 날짜 설정을 위해 임포트
import pandas as pd
import schedule
import time
import scheduler
# 메인 파이프라인 스크립트 함수 구현
def apt_api_documents_list() -> list[Document]:
    """
    아파트 실거래가 데이터를 수집하고,
    수집된 데이터를 바탕으로 Document 객체 리스트를 생성하여 반환합니다.
    """
    
    # --- 1. 데이터 수집 단계 (apt.py 호출) ---
    print("--- 1. 데이터 수집 스크립트(apt.py) 실행 ---")
    
    # 날짜 설정(현재년월 -> "YYYYMM" 형식)
    now = datetime.datetime.now()
    year = now.year
    month = now.month
    ym = f"{year}{month:02d}"
    
    # (테스트용 고정 날짜)
    # ym = "202510"
    
    # apt.py의 함수들을 호출하여 'total_df' 생성 (DataFrame)반환
    total_df = apt_sub.get_all_apt_trade_data(ym)
    
    if total_df.empty:
        print(f"=== {ym} 기간에 조회된 실거래가 데이터가 전혀 없습니다. ===")
        return []  # 빈 리스트 반환
    else:
        print(f"=== {ym} 기간의 실거래가 데이터 수집 완료: {len(total_df)} 건 ===")
        
        # --- 2. 텍스트 콘텐츠 생성 단계 (apt.py 호출) ---
        print("\n--- 2. 텍스트 콘텐츠(page_content) 생성 ---")
        
        # [중요] 변수명 수정: xml_strings -> text_strings
        # 이 리스트는 XML이 아니라 "2025년..."으로 시작하는 텍스트 문장입니다.
        text_strings: list[str] = apt_sub.return_apt_string(total_df)
        print(f"텍스트 콘텐츠 생성 완료: {len(text_strings)} 행")
        
        # --- 3. Document 생성 단계 ---
        print("\n--- 3. Document 객체 생성 (DataFrame -> Metadata) ---")
        
        # DataFrame을 딕셔너리 리스트로 변환 (메타데이터로 사용)
        # metadata_list 컬럼 (REGION_CODE(지역명), ENACTMENT_DATA(올라온 날짜), DOCUMENT_TYPE(xml), 이후는 db 에서 ROLE_ACCESS, UPDATE_TIMESTAMP, DOCUMENT_ID) 으로 총 5개 생성
        REGION_CODE = total_df['sggCd']
        ENACTMENT_DATA = total_df['dealYear'] + "/" +total_df['dealMonth'] + "/" + total_df['dealDay'] # 날짜 합치기 db에 날짜형식으로 저장
        metadata_list = []
        for i in range(len(total_df)):
            metadata = {
                "REGION_CODE": REGION_CODE.iloc[i],
                "ENACTMENT_DATA": ENACTMENT_DATA.iloc[i],
                "DOCUMENT_TYPE": "XML"
            }
            metadata_list.append(metadata)
        documents: list[Document] = []
        # 텍스트 리스트(page_content)와 딕셔너리 리스트(metadata)를 zip으로 결합
        for text_content, metadata_dict in zip(text_strings, metadata_list):
            
            # [안전장치] 메타데이터의 모든 값을 문자열로 변환
            # (VectorDB는 NaN 등 다른 타입을 싫어할 수 있음)
            cleaned_metadata = {key: str(value) if pd.notna(value) else 'N/A'  # NaN 처리 지역명이 직거래로 인해 없는 경우가 있음 
                                for key, value in metadata_dict.items()}
            
            # Document 객체 생성
            doc = Document(
                page_content=text_content,
                metadata=cleaned_metadata
            )
            documents.append(doc)
    
    return documents

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
    
    text_strings: list[str] = apt_sub.return_apt_string(total_df)

    filedate = f"{year}{month:02d}{day:02d}" # 파일명에 사용할 날짜 문자열 설정 -> YYYYMMDD
    filename = f"txts/apt_real_estate/apt_data_{filedate}.txt" # 파일명 설정 -> real_estate/apt_documents_YYYYMMDD.txt
    with open(filename, 'w', encoding='utf-8') as f:
        for i, text in enumerate(text_strings):
            f.write(text) # 텍스트 쓰기
            f.write("\n\n") # 각 문서 구분을 위한 빈 줄 추가
    print(f"텍스트 파일로 저장 완료: {filename}")

# 전원세 문자열 리스트를 텍스트 파일로 저장하는 함수 (스케줄링 가능)
def save_apt_rent_data_to_txt():
    """전원세 문자열 리스트를 텍스트 파일로 저장하는 함수"""
    now = datetime.datetime.now()
    year = now.year
    month = now.month
    ym = f"{year}{month:02d}"

    rent_data = apt_sub.get_all_apt_rent_data(ym)
    rent_strings = apt_sub.return_apt_rent_string(rent_data)

    filename = f"txts/apt_real_estate/apt_rent_data_{ym}.txt"
    with open(filename, 'w', encoding='utf-8') as f:
        for s in rent_strings:
            f.write(s + "\n\n")

# 스케줄링
# 15일 마다 자동 실행(주기 설정 나중에 변경 가능)
schedule.every(15).days.do(save_apt_rent_data_to_txt)
schedule.every(15).days.do(save_apt_data_to_txt)

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
    while True:
        schedule.run_pending()
        time.sleep(1)