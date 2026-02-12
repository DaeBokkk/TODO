import json
import re
import hashlib
from typing import List, Dict, Any

# LangChain 필수 모듈 임포트
from langchain_core.documents import Document
from langchain_text_splitters import RecursiveCharacterTextSplitter

# ------------------------------------------------------------------------------
# [설계 설정] 청킹 전략 매개변수 (뉴스 데이터 최적화)
# ------------------------------------------------------------------------------
# 512 토큰: LLM Context Window를 고려한 최적 크기
CHUNK_SIZE = 512
# 50 토큰: 뉴스 기사의 문맥 끊김 방지를 위한 중복 영역
CHUNK_OVERLAP = 50

# ------------------------------------------------------------------------------
# 1. 데이터 로딩 (Data Loading - JSONL 지원)
# ------------------------------------------------------------------------------
def load_raw_jsonl_file(file_path: str) -> List[Dict[str, Any]]:
    """
    JSONL (Line-delimited JSON) 파일을 읽어와 딕셔너리 리스트로 반환합니다.
    
    Args:
        file_path (str): 업로드된 .txt 파일 경로
        
    Returns:
        List[Dict]: 파싱된 뉴스 데이터 리스트
    """
    raw_data = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_number, line in enumerate(f):
                line = line.strip()
                if not line: continue # 빈 줄 무시
                
                try:
                    # 각 줄을 JSON 객체로 파싱
                    data_obj = json.loads(line)
                    raw_data.append(data_obj)
                except json.JSONDecodeError as e:
                    print(f"[Warning] Line {line_number+1}: JSON 파싱 실패 - {e}")
                    
        print(f"[System] 파일 로드 완료: 총 {len(raw_data)}개의 뉴스 기사를 읽었습니다.")
        return raw_data
        
    except FileNotFoundError:
        print(f"[Error] 파일을 찾을 수 없습니다: {file_path}")
        return []

# ------------------------------------------------------------------------------
# 2. 메타데이터 추출 및 정제 (Metadata Extraction & Cleaning)
# ------------------------------------------------------------------------------
def process_news_data(raw_item: Dict[str, Any], index: int) -> Dict[str, Any]:
    """
    JSON 객체에서 필요한 본문과 메타데이터를 추출하고 정제합니다.
    
    Args:
        raw_item (Dict): JSON으로 파싱된 원본 데이터 1건
        index (int): 임시 ID 생성을 위한 인덱스
        
    Returns:
        Dict: 정제된 텍스트(page_content)와 메타데이터(metadata)
    """
    # 1. 원본 데이터 구조 분해
    # 파일 구조: {"metadata": {...}, "content": "제목: ...\n내용: ..."}
    origin_meta = raw_item.get("metadata", {})
    content_text = raw_item.get("content", "")

    # 2. 본문 텍스트 정제
    # "제목:"과 "내용:" 태그를 제거하고 순수 텍스트로 결합하거나, 필요한 경우 분리
    # 여기서는 검색 효율을 위해 하나로 합치되 불필요한 태그만 제거합니다.
    cleaned_text = content_text.replace("제목:", "").replace("내용:", "\n")
    
    # 기자 이메일, 저작권 문구 등 뉴스 특유의 노이즈 제거
    cleaned_text = re.sub(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', '', cleaned_text) # 이메일 제거
    cleaned_text = re.sub(r'※ 이 기사의 저작권은.*', '', cleaned_text) # 저작권 문구 제거
    cleaned_text = re.sub(r'\/.*?=.*?기자', '', cleaned_text) # "/용인=김종성 기자" 패턴 제거
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip() # 다중 공백 제거

    # 3. 메타데이터 구성
    # 날짜 포맷팅 (YYYYMMDD -> YYYY-MM-DD)
    raw_date = origin_meta.get("enactment_date", "")
    formatted_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}" if len(raw_date) == 8 else raw_date

    # 고유 ID 생성 (해시 기반)
    doc_hash = hashlib.md5(content_text.encode()).hexdigest()[:10]
    # @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
    rdb_id = f"RENT_{index}_{doc_hash}"

    final_metadata = {
        "rdb_id": rdb_id,
        "contract_date": formatted_date,  # enactment_date를 계약일/발행일로 매핑
        "region_code": origin_meta.get("region_code")
    }

    return {
        "page_content": cleaned_text,
        "metadata": final_metadata
    }

# ------------------------------------------------------------------------------
# 3. Document 객체 생성 및 청킹 (Chunking)
# ------------------------------------------------------------------------------
def create_and_chunk_documents(raw_data_list: List[Dict[str, Any]]) -> List[Document]:
    """
    데이터 리스트를 받아 Document 객체로 만들고 청킹합니다.
    """
    documents = []
    
    # 3-1. Document 객체 생성
    for idx, item in enumerate(raw_data_list):
        processed = process_news_data(item, idx)
        doc = Document(
            page_content=processed["page_content"],
            metadata=processed["metadata"]
        )
        documents.append(doc)

    print(f"[System] Document 객체 생성 완료: {len(documents)}개")

    # 3-2. 청킹 수행
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=CHUNK_SIZE,
        chunk_overlap=CHUNK_OVERLAP,
        separators=["\n\n", "\n", ". ", " ", ""], # 문장 단위 분할 우선
        length_function=len
    )
    
    chunked_docs = text_splitter.split_documents(documents)
    print(f"[System] 청킹 완료: 총 {len(chunked_docs)}개의 청크가 생성되었습니다.")
    
    return chunked_docs

# ------------------------------------------------------------------------------
# 메인 실행 함수
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    # 파일 경로 (업로드한 파일명)
    file_path = 'apt_rent_data_20260111.txt'
    
    print("--- [1단계] JSONL 파일 로드 ---")
    raw_data = load_raw_jsonl_file(file_path)
    
    if raw_data:
        print("\n--- [2단계] 정제 및 청킹 ---")
        final_chunks = create_and_chunk_documents(raw_data)
        
        print("\n--- [3단계] 결과 검증 (샘플 출력) ---")
        for i, chunk in enumerate(final_chunks[:]): # 처음 2개만 출력
            print(f"\n[Chunk #{i+1}]")
            print(f"ID: {chunk.metadata.get('rdb_id')}")
            print(f"enactment_date: {chunk.metadata.get('contract_date')}")
            print(f"region_code: {chunk.metadata.get('region_code')}")
            print(f"Content: {chunk.page_content[:]}...") # 앞 100자만 출력


            #  query{
            #     main_intent : 질문의도
            #     location : 위치
            #     complex_name  : 아파트 단지명
            #     property_type : 부동산 속성 (오피,아파트)
            #     price_metric : 가격
            #     period : 조회기간
            # }