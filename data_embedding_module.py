import os
import time
from typing import List

# LangChain 필수 라이브러리 임포트
from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_community.vectorstores import PGVector

# 이전 단계에서 만든 청킹 모듈을 가져옵니다. (파일명이 data_chunking_module.py라고 가정)
# 만약 같은 폴더에 파일이 없다면, 아래 main 함수 내의 더미 데이터 생성 로직을 사용하세요.
try:
    from data_chunking_module import load_raw_text_file, create_and_chunk_documents
except ImportError:
    print("[System] 청킹 모듈을 찾을 수 없습니다.")

# ------------------------------------------------------------------------------
# [설계 설정] 임베딩 모델 및 DB 설정 
# ------------------------------------------------------------------------------

# 1. 사용할 임베딩 모델 ID (KO-SBERT)
# 한국어 문장 의미 파악에 최적화된 오픈소스 모델입니다.
MODEL_ID = "jhgan/ko-sbert-nli"
MODEL_DEVICE = "cpu" # GPU가 있다면 "cuda"로 변경 (Mac M1/M2는 "mps" 사용 가능)

# 2. PostgreSQL DB 연결 정보 (DB 담당자에게 받은 접속 정보)
# 형식: postgresql://user:password@host:port/dbname
DB_CONNECTION_STRING = "postgresql://llm_user:rag_db_password@localhost:5432/rag_project_db"

# 3. 벡터 DB 인덱스(테이블) 이름
# KO-SBERT 전용 인덱스임을 명시합니다.
COLLECTION_NAME = "vector_index_ko_sbert"


# ------------------------------------------------------------------------------
# 1. 임베딩 모델 로드 함수 (Embedding Model Loading)
# ------------------------------------------------------------------------------
def load_embedding_model() -> Embeddings:
    """
    KO-SBERT 임베딩 모델을 메모리에 로드하여 LangChain 객체로 반환합니다.

    """
    print(f"\n[System] 임베딩 모델 로딩 시작: {MODEL_ID} ...")
    start_time = time.time()

    # HuggingFaceEmbeddings를 사용하여 로컬 모델을 로드한다.
    embeddings = HuggingFaceEmbeddings(
        model_name=MODEL_ID,
        model_kwargs={'device': MODEL_DEVICE}, # CPU/GPU 설정
        encode_kwargs={'normalize_embeddings': True} # 코사인 유사도를 위해 정규화
    )

    end_time = time.time()
    print(f"[System] 모델 로딩 완료 (소요시간: {end_time - start_time:.2f}초)")
    
    return embeddings


# ------------------------------------------------------------------------------
# 2. 벡터 DB 저장 함수 (Indexing / Ingestion)
# ------------------------------------------------------------------------------
def save_to_vector_db(documents: List[Document], embeddings: Embeddings):
    """
    청킹된 Document 리스트를 받아 벡터화한 후, PostgreSQL(pgvector)에 저장한다.
    
    Args:
        documents: 청킹과 메타데이터 부착이 완료된 Document 객체 리스트
        embeddings: 로드된 임베딩 모델 객체
    """
    if not documents:
        print("[Warning] 저장할 문서가 없습니다.")
        return

    print(f"\n[System] 벡터 DB 저장 시작 (총 {len(documents)}개 청크)")
    print(f" -> 대상 DB 인덱스: {COLLECTION_NAME}")
    
    try:
        # PGVector.from_documents 메서드 사용
        # 1. documents의 텍스트를 embeddings 모델로 벡터화합니다.
        # 2. 벡터, 원본 텍스트, 메타데이터를 DB 테이블에 삽입(INSERT)합니다.
        # 3. 테이블이 없으면 자동으로 생성합니다.
        
        db = PGVector.from_documents(
            embedding=embeddings,
            documents=documents,
            collection_name=COLLECTION_NAME,
            connection_string=DB_CONNECTION_STRING,
            pre_delete_collection=False # True로 하면 기존 데이터를 지우고 새로 만듭니다.
        )
        
        print("[Success] 모든 데이터가 성공적으로 벡터 DB에 저장(색인)되었습니다.")
        
    except Exception as e:
        print(f"[Error] DB 저장 중 오류 발생: {e}")
        print(" -> 팁: 서버가 실행 중인지, pgvector 확장이 켜져 있는지 확인하세요.")


# ------------------------------------------------------------------------------
# 메인 실행 함수 (Pipeline Orchestration)
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    # 1. 데이터 준비 (청킹 모듈 호출)
    # 실제 파일이 있다면 로드하고, 없다면 테스트 데이터를 만든다.
    file_path = 'apt_data_20251121.txt'
    
    if os.path.exists(file_path):
        # 이전 모듈 활용
        print("--- [Step 1] 원본 파일 로드 및 청킹 ---")
        raw_texts = load_raw_text_file(file_path)
        final_chunks = create_and_chunk_documents(raw_texts)
    else:
        # 테스트용 더미 데이터 (파일이 없을 경우)
        print("--- [Step 1] 테스트용 더미 데이터 생성 ---")
        final_chunks = [
            Document(
                page_content="테스트용 아파트 시세 팩트입니다. 2025년 10월 5억 거래됨.",
                metadata={"rdb_id": "TEST_001", "contract_date": "2025-10-01"}
            ),
            Document(
                page_content="이것은 두 번째 테스트 청크입니다. KO-SBERT 성능 확인용.",
                metadata={"rdb_id": "TEST_002", "source": "dummy"}
            )
        ]

    # 2. 임베딩 모델 준비
    print("\n--- [Step 2] KO-SBERT 임베딩 모델 준비 ---")
    ko_sbert_model = load_embedding_model()

    # 3. DB에 벡터화하여 저장 (색인 실행)
    print("\n--- [Step 3] 벡터화 및 DB 저장 실행 ---")
    save_to_vector_db(final_chunks, ko_sbert_model)

### **코드 실행 가이드 (LLM 담당자용)**

# 1.  **사전 준비**:
#     * `data_chunking_module.py` 파일이 같은 폴더에 있어야 합니다. (없으면 위 코드의 `else` 블록이 더미 데이터로 작동합니다.)
#     * `docker-compose up -d`로 **PostgreSQL 컨테이너가 실행 중**이어야 합니다.
# 2.  **실행**:
#     ```bash
#     python embedding_module.py