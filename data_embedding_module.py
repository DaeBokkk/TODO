import os
import time
import psycopg # psycopg3 (binary 설치 필요)
from typing import List

# LangChain 필수 라이브러리
from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_community.vectorstores import PGVector

try:
    from data_chunking_module import load_raw_jsonl_file, create_and_chunk_documents
except ImportError:
    pass # 메인 실행이 아닐 경우를 대비해 에러 무시

# ------------------------------------------------------------------------------
# [설계 설정] 임베딩 모델 및 DB 설정
# ------------------------------------------------------------------------------

# 1. 사용할 임베딩 모델 ID (KO-SBERT) ID: jhgan/ko-sbert-nli
MODEL_ID = "jhgan/ko-sbert-nli"
MODEL_DEVICE = "cpu" 

# 2. PostgreSQL DB 연결 정보
DB_HOST = "0.tcp.jp.ngrok.io"
DB_PORT = "18648"
DB_USER = "rag"
DB_PASSWORD = "rag"
DB_NAME = "rag"

DB_CONNECTION_STRING = f"host={DB_HOST} port={DB_PORT} user={DB_USER} password={DB_PASSWORD} dbname={DB_NAME}"
# DB_CONNECTION_STRING = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# 3. 벡터 DB 인덱스(테이블) 이름
COLLECTION_NAME = "embedding_vector_ko_sbert"

# 1. 임베딩 모델 로드 함수 (Embedding Model Loading)

def load_embedding_model() -> Embeddings:
    """
    KO-SBERT 임베딩 모델을 메모리에 로드하여 LangChain 객체로 반환한다.
    """
    print(f"\n[System] 임베딩 모델 로딩 시작: {MODEL_ID} ...")
    start_time = time.time()

    # HuggingFaceEmbeddings를 사용하여 로컬 모델을 로드한다.
    # encode_kwargs={'normalize_embeddings': True}는 코사인 유사도 계산 정확도를 높여준다.

    embeddings = HuggingFaceEmbeddings(
        model_name=MODEL_ID,
        model_kwargs={'device': MODEL_DEVICE},
        encode_kwargs={'normalize_embeddings': True}
    )

    end_time = time.time()
    print(f"[System] 모델 로딩 완료 (소요시간: {end_time - start_time:.2f}초)")

    return embeddings
# # ------------------------------------------------------------------------------
# # 3. [핵심 수정] 기존 테이블에 데이터 저장 함수 (Custom Insert)
# # ------------------------------------------------------------------------------
# def save_to_existing_table(documents: List[Document], embeddings: Embeddings):
#     """
#     LangChain의 기본 PGVector 함수 대신, Raw SQL을 사용하여
#     우리가 미리 만든 'DOCUMENT_CHUNK' 테이블에 직접 데이터를 삽입합니다.
#     """
#     if not documents:
#         print("[Warning] 저장할 문서가 없습니다.")
#         return

#     print(f"\n[System] 데이터 임베딩 및 DB 저장 시작 (총 {len(documents)}개 청크)")
    
#     # 1. 텍스트 리스트 추출 (벡터화용)
#     texts = [doc.page_content for doc in documents]
    
#     # 2. 일괄 임베딩 (Batch Embedding) - 속도 최적화
#     print(" -> 텍스트 벡터화 진행 중...")
#     start_embed = time.time()
#     try:
#         vectors = embeddings.embed_documents(texts)
#     except Exception as e:
#         print(f"[Error] 임베딩 실패: {e}")
#         return
#     print(f" -> 벡터화 완료 ({time.time() - start_embed:.2f}초)")

#     # [확인용 출력] 첫 번째 데이터의 벡터값 일부 출력
#     if vectors:
#         print(f"** [Vector Sample] 첫 번째 청크 벡터(앞 5개): {vectors[0][:5]} ... (총 768차원)")

#     # 3. DB 연결 및 Insert (SQL 실행)
    
#     # ERD 컬럼명에 맞춘 INSERT 쿼리 
#         insert_sql = PGVector.from_documents(

#             embedding=embeddings,
#             documents=documents,
#             collection_name=COLLECTION_NAME,
#             connection_string=DB_CONNECTION_STRING,
#             pre_delete_collection=False # 기존 데이터를 유지하고 추가.
#             )
        
#     success_count = 0
    
#     try:
#         with psycopg.connect(DB_CONNECTION_STRING) as conn:
#             # 안전장치 1: pgvector 확장 활성화 확인
#             with conn.cursor() as cur:
#                  cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
            
#             print(" -> DB 삽입(Insert) 시작...")
            
#             with conn.cursor() as cur:
#                 # 쿼리 실행
#                 for i, doc in enumerate(documents):
#                     metadata = doc.metadata
#                     vector = vectors[i]
                    
#                     # 파라미터 매핑 (메타데이터 -> DB 컬럼)
#                     # 메타데이터 키는 대소문자 구분 없이 가져오도록 설정
#                     rdb_id = metadata.get("rdb_id") or metadata.get("RDB_ID") or ""
#                     region_code = metadata.get("region_code") or metadata.get("REGION_CODE") or ""
#                     contract_date = metadata.get("contract_date") or metadata.get("ENACTMENT_DATE") or ""
                    
#                     # 쿼리 실행 시 ERD 컬럼 순서와 타입에 맞춰 파라미터를 제공
#                     cur.execute(insert_sql, (
#                         rdb_id,
#                         doc.page_content,
#                         region_code,
#                         contract_date,
#                         vector
#                     ))
#                     success_count += 1
                
#                 conn.commit() # 최종 커밋

#         print(f"[Success] 총 {success_count}건이 기존 테이블 '{COLLECTION_NAME}'에 저장되었습니다.")

#     except Exception as e:
#         print(f"❌ [DB Error] 데이터 저장 실패: {e}")
#         print(" -> 힌트: 'DOCUMENT_CHUNK' 테이블의 컬럼명과 데이터 타입이 이 코드의 INSERT SQL과 일치하는지 확인하세요.")
#         print(" -> 힌트: Ngrok 포트 번호가 정확한지 확인하세요.")
# 2. 벡터 DB 저장 함수
def save_to_vector_db(documents: List[Document], embeddings: Embeddings):
    """
    청킹된 Document 리스트를 받아 벡터화한 후, PostgreSQL(pgvector)에 저장한다.
    """
    if not documents:
        print("[Warning] 저장할 문서가 없습니다.")
        return

    print(f"\n[System] 벡터 DB 저장 시작 (총 {len(documents)}개 청크)")
    print(f" -> 대상 DB: {DB_HOST}:{DB_PORT} ({COLLECTION_NAME})")

    try:
        # PGVector.from_documents 메서드 사용
        # 이 함수는 내부적으로 '임베딩 -> 벡터 변환 -> DB 연결 -> 데이터 삽입'을 모두 수행.
        db = PGVector.from_documents(
            embedding=embeddings,
            documents=documents,
            collection_name=COLLECTION_NAME,
            connection_string=DB_CONNECTION_STRING,
            pre_delete_collection=False # 기존 데이터를 유지하고 추가.
            )
        
        print("[Success] 모든 데이터가 성공적으로 벡터 DB에 저장(색인)되었습니다.")

    except Exception as e:
        print(f"[Error] DB 저장 중 오류 발생: {e}")
        print(" -> 팁: Ngrok 터널이 열려 있는지, DB 계정 정보가 정확한지 확인하세요.")

#  3. 사용자 질문 임베딩 함수 (Query Embedding)

def embed_user_query(query: str, embeddings: Embeddings) -> List[float]:
    """
    사용자의 자연어 질문을 입력받아, 검색(Retrieval)에 사용할 벡터로 변환한다.
    
    Args:
        query (str): 사용자의 질문 텍스트 (예: "강남구 아파트 시세 알려줘")
        embeddings (Embeddings): 로드된 임베딩 모델 객체
        
    Returns:
        List[float]: 변환된 벡터 (숫자 리스트)
    """
    print(f"\n[System] 사용자 질문 벡터화 시작: '{query}'")
    
    try:
        # LangChain Embeddings 객체의 embed_query 메서드 사용
        query_vector = embeddings.embed_query(query)
        
        vector_dim = len(query_vector)
        print(f"[Success] 질문이 벡터로 변환되었습니다. (차원: {vector_dim})")

        return query_vector
        
    except Exception as e:
        print(f"[Error] 질문 벡터화 중 오류 발생: {e}")

        return []

    # 메인 실행 함수 (Pipeline Orchestration)
if __name__ == "__main__":

# 1. 임베딩 모델 준비 (한 번 로드하여 재사용)
    ko_sbert_model = load_embedding_model()

    # --- 데이터 색인 (지식 기반 구축) ---
    file_path = 'bitkinds_news_20251126.txt'

    # 파일이 존재하면 로드 및 색인 진행
    if os.path.exists(file_path):
        print("\n--- [Mode 1] 지식 기반 구축 (데이터 색인) ---")

        raw_texts = load_raw_jsonl_file(file_path)
        final_chunks = create_and_chunk_documents(raw_texts)

        # DB에 저장
        save_to_vector_db(final_chunks, ko_sbert_model)
    else:
        print(f"\n[Info] '{file_path}' 파일이 없어 데이터 색인 과정을 건너뜁니다.")

    # --- 사용자 질문 테스트 (실시간 추론 시뮬레이션) ---
    print("\n--- [Mode 2] 사용자 질문 임베딩 테스트 ---")
    test_question = "강남 반포자이 최근 6개월 전세 시세 알려줘."
    
    # 질문 벡터화 함수 호출
    vector_result = embed_user_query(test_question, ko_sbert_model)
    
    # 결과 확인 (앞부분 5개 수치만 출력)
    if vector_result:
        print(f"생성된 벡터 예시 (앞 5개): {vector_result[:5]} ...")


