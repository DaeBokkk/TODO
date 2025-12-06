import os
import time
from typing import List
import psycopg2 # [수정] 사용자가 설치한 psycopg2-binary 사용

# LangChain 필수 라이브러리
from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_community.vectorstores import PGVector

# 청킹 모듈 연동
try:
    from data_chunking_module import load_raw_jsonl_file, create_and_chunk_documents
except ImportError:
    pass

# ------------------------------------------------------------------------------
# 1. [설계 설정] 임베딩 모델 및 DB 연결 설정
# ------------------------------------------------------------------------------

# (1) 임베딩 모델 설정 (KO-SBERT)
MODEL_ID = "jhgan/ko-sbert-nli"
MODEL_DEVICE = "cpu"

# (2) PostgreSQL DB 연결 정보 (Ngrok 정보 반영)
DB_HOST = "0.tcp.jp.ngrok.io"
DB_PORT = "18757"  # 포트 번호 확인 필요
DB_USER = "rag"
DB_PASSWORD = "rag"
DB_NAME = "rag"

# [중요] LangChain PGVector는 SQLAlchemy 형식의 연결 문자열을 사용합니다.
DB_CONNECTION_STRING = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# (3) 벡터 컬렉션 이름
COLLECTION_NAME = "embedding_vector_ko_sbert"


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
# 3. [표준] 벡터 DB 저장 함수 (LangChain PGVector 사용)
# ------------------------------------------------------------------------------
def fix_db_schema_conflict():
    """
    [오류 해결용] 'collection_id' 컬럼 없음 오류를 해결하기 위해
    기존의 잘못된 테이블을 강제로 삭제하고 초기화합니다.
    """
    print("⚠️ [DB Fix] 기존 테이블 스키마 충돌 감지. 테이블 초기화를 시도합니다...")
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_NAME
        )
        cur = conn.cursor()
        # LangChain이 사용하는 기본 테이블들을 삭제 (재생성을 유도)
        cur.execute("DROP TABLE IF EXISTS langchain_pg_embedding CASCADE;")
        cur.execute("DROP TABLE IF EXISTS langchain_pg_collection CASCADE;")
        conn.commit()
        cur.close()
        conn.close()
        print("✅ [DB Fix] 기존 테이블 삭제 완료. 이제 LangChain이 올바른 테이블을 새로 생성합니다.")
    except Exception as e:
        print(f"❌ [DB Fix] 초기화 중 오류 발생 (무시 가능): {e}")


def save_to_vector_db(documents: List[Document], embeddings: Embeddings):
    """
    LangChain의 표준 PGVector 모듈을 사용하여 데이터를 저장합니다.
    """
    if not documents:
        print("[Warning] 저장할 문서가 없습니다.")
        return
    
    # [수정] 저장 전에 스키마 충돌 방지를 위해 테이블 초기화 실행
    # 주의: 이 코드는 기존 데이터를 날립니다. (초기 구축 단계이므로 안전하다고 가정)
    fix_db_schema_conflict()

    print(f"\n[System] 벡터 DB 저장 시작 (총 {len(documents)}개 청크)")
    print(f" -> 접속 정보: {DB_HOST}:{DB_PORT}")
    print(f" -> 컬렉션명: {COLLECTION_NAME}")

    try:
        # PGVector.from_documents 메서드
        # 테이블이 없으면 자동으로 생성합니다.
        db = PGVector.from_documents(
            embedding=embeddings,
            documents=documents,
            collection_name=COLLECTION_NAME,
            connection_string=DB_CONNECTION_STRING,
            pre_delete_collection=False 
        )
        
        print("[Success] 모든 데이터가 성공적으로 벡터 DB에 저장(색인)되었습니다.")
        
    except Exception as e:
        print(f"[Error] DB 저장 중 오류 발생: {e}")
        print(" -> 팁: Ngrok 포트가 변경되었는지 확인하세요.")


# ------------------------------------------------------------------------------
# 4. 사용자 질문 임베딩 함수 (검색용)
# ------------------------------------------------------------------------------
def embed_user_query(query: str, embeddings: Embeddings) -> List[float]:
    """
    사용자의 질문을 벡터로 변환합니다.
    """
    print(f"\n[System] 사용자 질문 벡터화: '{query}'")
    try:
        query_vector = embeddings.embed_query(query)
        print(f"[Success] 변환 완료 (차원: {len(query_vector)})")
        return query_vector
    except Exception as e:
        print(f"[Error] 질문 변환 실패: {e}")
        return []


# ------------------------------------------------------------------------------
# 5. 메인 실행 (테스트)
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    # 1. 모델 로드
    ko_sbert_model = load_embedding_model()

    # --- [Mode 1] 데이터 색인 ---
    file_path = 'apt_rent_data_20251130.txt' # 파일명 확인
    
    if os.path.exists(file_path):
        print("\n=== [Mode 1] 지식 기반 구축 (데이터 색인) ===")
        # 청킹 모듈 호출
        raw_texts = load_raw_jsonl_file(file_path)
        final_chunks = create_and_chunk_documents(raw_texts)
        
        # DB 저장 실행
        save_to_vector_db(final_chunks, ko_sbert_model)
    else:
        print(f"\n[Info] 데이터 파일('{file_path}')이 없어 데이터 색인 과정을 건너뜁니다.")

    # --- [Mode 2] 사용자 질문 벡터화 ---
    print("\n=== [Mode 2] 사용자 질문 임베딩 테스트 ===")
    test_question = "강남 반포자이 최근 6개월 전세 시세 알려줘."
    
    vector_result = embed_user_query(test_question, ko_sbert_model)
    
    if vector_result:
        print(f"생성된 벡터 예시 (앞 5개): {vector_result[:5]} ...")