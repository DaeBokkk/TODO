import os
import time
import psycopg # psycopg3 (binary 설치 필요)
from typing import List

# LangChain 필수 라이브러리
from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings
from langchain_community.embeddings import HuggingFaceEmbeddings

# 이전 단계에서 만든 청킹 모듈 가져오기
try:
    from data_chunking_module import load_raw_jsonl_file, create_and_chunk_documents
except ImportError:
    pass

# ------------------------------------------------------------------------------
# 1. [설계 설정] 임베딩 모델 및 DB 연결 설정
# ------------------------------------------------------------------------------

# (1) 임베딩 모델 설정
MODEL_ID = "jhgan/ko-sbert-nli"
MODEL_DEVICE = "cpu" 

# (2) DB 연결 정보 (Ngrok 정보 반영)
DB_HOST = "0.tcp.jp.ngrok.io"
DB_PORT = "17339"
DB_USER = "rag"
DB_PASSWORD = "rag"
DB_NAME = "rag"

# psycopg 연결 문자열 (Connection Info)
DB_CONN_INFO = f"host={DB_HOST} port={DB_PORT} user={DB_USER} password={DB_PASSWORD} dbname={DB_NAME}"

# (3) 타겟 테이블 설정 (ERD 기준)
TARGET_TABLE = "embedding_vector_ko_sbert"


# ------------------------------------------------------------------------------
# 2. 임베딩 모델 로드 함수
# ------------------------------------------------------------------------------
def load_embedding_model() -> Embeddings:
    """
    KO-SBERT 모델을 메모리에 로드합니다.
    """
    print(f"\n[System] 임베딩 모델 로딩 중... ({MODEL_ID})")
    start = time.time()
    
    embeddings = HuggingFaceEmbeddings(
        model_name=MODEL_ID,
        model_kwargs={'device': MODEL_DEVICE},
        encode_kwargs={'normalize_embeddings': True}
    )
    
    print(f"[System] 로딩 완료 ({time.time() - start:.2f}초)")
    return embeddings


# ------------------------------------------------------------------------------
# 3. [핵심 수정] 기존 테이블에 데이터 저장 함수 (Custom Insert)
# ------------------------------------------------------------------------------
def save_to_existing_table(documents: List[Document], embeddings: Embeddings):
    """
    LangChain의 기본 저장 방식을 쓰지 않고, 
    우리가 미리 만든 'DOCUMENT_CHUNK' 테이블에 직접 데이터를 삽입합니다.
    """
    if not documents:
        print("[Warning] 저장할 문서가 없습니다.")
        return

    print(f"\n[System] 데이터 임베딩 및 DB 저장 시작 (총 {len(documents)}개 청크)")
    
    # 1. 텍스트 리스트 추출 (벡터화용)
    texts = [doc.page_content for doc in documents]
    
    # 2. 일괄 임베딩 (Batch Embedding) - 속도 최적화
    print(" -> 텍스트 벡터화 진행 중...")
    start_embed = time.time()
    try:
        vectors = embeddings.embed_documents(texts)
    except Exception as e:
        print(f"[Error] 임베딩 실패: {e}")
        return
    print(f" -> 벡터화 완료 ({time.time() - start_embed:.2f}초)")

    # 3. DB 연결 및 Bulk Insert (SQL 실행)
    # pgvector는 리스트 형태의 벡터를 자동으로 인식합니다.
    
    # ERD 컬럼명에 맞춘 INSERT 쿼리
    insert_sql = f"""
        INSERT INTO {TARGET_TABLE} (
            DOCUMENT_ID, 
            CHUNK_TEXT, 
            REGION_CODE, 
            ENACTMENT_DATE, 
            EMBEDDING_VECTOR_KO_SBERT,  -- 모델별 전용 컬럼
            UPDATE_TIMESTAMP
        ) VALUES (
            %s, %s, %s, %s, %s, NOW()
        )
    """
    
    success_count = 0
    
    try:
        # psycopg context manager 사용 (자동 commit/close)
        with psycopg.connect(DB_CONN_INFO) as conn:
            with conn.cursor() as cur:
                
                print(" -> DB 삽입(Insert) 시작...")
                
                # executemany를 쓰면 더 빠르지만, 디버깅을 위해 루프로 처리합니다.
                for i, doc in enumerate(documents):
                    metadata = doc.metadata
                    vector = vectors[i]
                    
                    # 파라미터 매핑 (메타데이터 -> DB 컬럼)
                    # 주의: 메타데이터 키 이름이 대소문자를 구분할 수 있으니 확인 필요
                    # 여기서는 .get()을 써서 안전하게 가져옵니다.
                    
                    rdb_id = metadata.get("rdb_id") or metadata.get("RDB_ID")
                    region_code = metadata.get("region_code") or metadata.get("REGION_CODE")
                    contract_date = metadata.get("contract_date") or metadata.get("ENACTMENT_DATA")
                    
                    # 날짜 포맷 정리 (YYYYMMDD -> YYYY-MM-DD) 등 필요시 변환 로직 추가
                    
                    cur.execute(insert_sql, (
                        rdb_id,
                        doc.page_content,
                        region_code,
                        contract_date,
                        vector
                    ))
                    success_count += 1
                
                conn.commit() # 최종 커밋

        print(f"[Success] 총 {success_count}건이 '{TARGET_TABLE}' 테이블에 저장되었습니다.")

    except Exception as e:
        print(f"[Error] DB 저장 실패: {e}")
        print(" -> 힌트: DOCUMENT_CHUNK 테이블이 미리 생성되어 있어야 합니다.")
        print(" -> 힌트: 컬럼명이나 데이터 타입이 맞는지 확인하세요.")


# ------------------------------------------------------------------------------
# 4. 사용자 질문 임베딩 함수 (기존 유지)
# ------------------------------------------------------------------------------
def embed_user_query(query: str, embeddings: Embeddings) -> List[float]:
    # ... (기존 코드와 동일)
    print(f"\n[System] 사용자 질문 벡터화: '{query}'")
    try:
        query_vector = embeddings.embed_query(query)
        print(f"[Success] 변환 완료 (차원: {len(query_vector)})")
        return query_vector
    except Exception as e:
        print(f"[Error] 질문 변환 실패: {e}")
        return []


# ------------------------------------------------------------------------------
# 메인 실행 (테스트)
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    # 1. 모델 로드
    ko_sbert_model = load_embedding_model()

    # --- [Mode 1] 데이터 색인 ---
    file_path = 'apt_rent_data_20251130.txt' # 파일명 확인 필요
    
    if os.path.exists(file_path):
        print("\n=== [Mode 1] 지식 기반 구축 (데이터 색인) ===")
        raw_texts = load_raw_jsonl_file(file_path)
        final_chunks = create_and_chunk_documents(raw_texts)
        
        # [수정된 함수 호출] 커스텀 테이블 저장 함수 사용
        save_to_existing_table(final_chunks, ko_sbert_model)
    else:
        print(f"\n[Info] 데이터 파일이 없어 색인 과정을 건너뜁니다.")