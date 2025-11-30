import re
from typing import List, Dict, Any
from datetime import datetime
import hashlib

# LangChain 필수 모듈 임포트
# Document: 텍스트와 메타데이터를 담는 기본 객체
from langchain_core.documents import Document
# RecursiveCharacterTextSplitter: 문맥을 유지하며 텍스트를 자르는 도구
from langchain_text_splitters import RecursiveCharacterTextSplitter

# ------------------------------------------------------------------------------
# [설계 설정] 청킹 전략 매개변수
# ------------------------------------------------------------------------------
# 512 토큰: LLM Context Window를 고려한 최적 크기 (환각 억제 및 검색 효율성)
CHUNK_SIZE = 512
# 50 토큰: 청크 간 문맥 끊김 방지를 위한 최소 중복 영역
CHUNK_OVERLAP = 50

# ------------------------------------------------------------------------------
# 1. 데이터 로딩 및 파싱 (Data Loading)
# ------------------------------------------------------------------------------
def load_raw_text_file(file_path: str) -> List[str]:
    """
    텍스트 파일을 읽어와 각 거래 내역(단락)별로 분리하여 리스트로 반환한다.
    
    Args:
        file_path (str): 업로드된 텍스트 파일 경로
        
    Returns:
        List[str]: 분리된 원본 텍스트 리스트
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
        # 데이터가 빈 줄(\n\n)로 구분되어 있다고 가정하고 분리합니다.
        # strip()으로 앞뒤 공백 제거 후, 비어있지 않은 항목만 필터링
        raw_records = [record.strip() for record in content.split('\n\n') if record.strip()]
        
        print(f"[System] 파일 로드 완료: 총 {len(raw_records)}개의 거래 내역을 읽었습니다.")
        return raw_records
        
    except FileNotFoundError:
        print(f"[Error] 파일을 찾을 수 없습니다: {file_path}")
        return []

# ------------------------------------------------------------------------------
# 2. 메타데이터 추출 및 정제 (Metadata Extraction & Cleaning)
# ------------------------------------------------------------------------------
def extract_metadata_and_clean(text: str, index: int) -> Dict[str, Any]:
    """
    원본 텍스트에서 메타데이터(날짜, 가격, ID)를 추출하고 텍스트를 정제합니다.
    (메타데이터 무결성 확보를 위한 필수 단계)
    
    Args:
        text (str): 거래 내역 원본 텍스트
        index (int): 임시 RDB ID 생성을 위한 인덱스
        
    Returns:
        Dict: 정제된 텍스트와 추출된 메타데이터를 포함한 딕셔너리
    """
    
    # [메타데이터 추출 1] 계약일 추출 (YYYY년 MM월 DD일 패턴)
    date_match = re.search(r'(\d{4})년 (\d{1,2})월 (\d{1,2})일', text)
    contract_date = "Unknown"
    if date_match:
        # 표준 날짜 형식(YYYY-MM-DD)으로 변환하여 메타데이터로 사용
        year, month, day = date_match.groups()
        contract_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
    
    # [메타데이터 추출 2] 거래 금액 추출 (숫자 검증용)
    # 예: "12억 3,000만원" -> 숫자만 추출하거나 정규화
    price_match = re.search(r'거래금액\s+([0-9억\s,]+)원', text)
    price_raw = price_match.group(1) if price_match else "Unknown"
    
    # [메타데이터 추출 3] 고유 ID 생성
    # 실제로는 DB의 Primary Key를 써야 하지만, 파일 처리이므로 해시값이나 인덱스로 대체
    # 여기서는 텍스트 내용 기반의 해시값을 생성하여 고유 ID로 사용 (중복 방지)
    doc_hash = hashlib.md5(text.encode()).hexdigest()[:10]
    rdb_id = f"TX_{index}_{doc_hash}"

    # [텍스트 정제]
    # LLM 검색에 방해가 될 수 있는 불필요한 특수문자나 패턴 제거
    cleaned_text = text.replace("'", "") # 따옴표 제거
    cleaned_text = re.sub(r"\s+", " ", cleaned_text).strip() # 다중 공백 제거
    
    return {
        "page_content": cleaned_text,
        "metadata": {
            "rdb_id": rdb_id,              # 2차 검증을 위한 핵심 연결 고리
            "contract_date": contract_date, # 출처 명시용 날짜
            "price_raw": price_raw,         # 원본 가격 정보 보존
            "source": "apt_data_file"       # 데이터 출처 표기
        }
    }

# ------------------------------------------------------------------------------
# 3. Document 객체 생성 및 청킹 (Document Creation & Chunking)
# ------------------------------------------------------------------------------
def create_and_chunk_documents(raw_records: List[str]) -> List[Document]:
    """
    정제된 데이터를 LangChain Document 객체로 변환하고, 청킹을 수행한다.
    """
    
    # Document 객체 생성 (LangChain 포맷으로 변환)
    documents = []
    for idx, record in enumerate(raw_records):
        processed_data = extract_metadata_and_clean(record, idx)
        
        doc = Document(
            page_content=processed_data["page_content"],
            metadata=processed_data["metadata"]
        )
        documents.append(doc)
    
    print(f"[System] Document 객체 생성 완료: {len(documents)}개")

    # 청킹 (Text Splitting)
    # LLM 담당자가 설계한 전략(512/50)을 적용한다.
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=CHUNK_SIZE,
        chunk_overlap=CHUNK_OVERLAP,
        # 한국어 문맥을 고려한 분할 기준 (줄바꿈 -> 마침표 -> 공백 순)
        separators=["\n\n", "\n", ".", " ", ""],
        length_function=len,
        is_separator_regex=False
    )
    
    # split_documents 함수는 청킹을 수행하면서 메타데이터를 자동으로 복제해준다.
    # 즉, 쪼개진 청크들도 원본의 rdb_id를 그대로 가진다. (메타데이터 무결성 유지)
    chunked_docs = text_splitter.split_documents(documents)
    
    print(f"[System] 청킹 완료: 총 {len(chunked_docs)}개의 청크가 생성되었습니다.")
    
    return chunked_docs

# ------------------------------------------------------------------------------
# 메인 실행 함수
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    # 1. 파일 경로 설정 (업로드된 파일명)
    file_path = 'apt_data_20251121.txt'
    
    print("--- [1단계] 데이터 로드 시작 ---")
    raw_data = load_raw_text_file(file_path)
    
    if raw_data:
        print("\n--- [2단계] 정제, 메타데이터 부착 및 청킹 시작 ---")
        final_chunks = create_and_chunk_documents(raw_data)
        
        print("\n--- [3단계] 최종 청크 결과 검증 (샘플 3개 출력) ---")
        # 결과 확인: 여기서 메타데이터가 잘 붙었는지, 텍스트가 잘 잘렸는지 확인한다.
        for i, chunk in enumerate(final_chunks[:3]):
            print(f"\n[청크 #{i+1}]")
            print(f"내용 길이: {len(chunk.page_content)} 자")
            print(f"RDB ID (검증용): {chunk.metadata.get('rdb_id')}")
            print(f"계약일 (출처용): {chunk.metadata.get('contract_date')}")
            print(f"본문 내용: {chunk.page_content}")
            print("-" * 50)
            
        print("\n[완료] final_chunks 리스트를 임베딩 모듈로 전달하기")