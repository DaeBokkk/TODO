import os
import time
import glob  # 폴더 내 여러 파일을 찾기 위한 라이브러리
import uuid
import json
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
# DB 정보

# (1) 임베딩 모델 설정 (KO-SBERT)
MODEL_ID = "jhgan/ko-sbert-nli"
MODEL_DEVICE = "cpu"

# (2) PostgreSQL DB 연결 정보 (Ngrok 정보 반영)
DB_HOST = "0.tcp.jp.ngrok.io"
DB_PORT = "16069"  # 포트 번호 확인 필요
DB_USER = "rag"
DB_PASSWORD = "rag"
DB_NAME = "rag"

# [중요] LangChain PGVector는 SQLAlchemy 형식의 연결 문자열을 사용합니다.
# DB_CONNECTION_STRING = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# (3) [중요] 타겟 테이블 및 컬럼 설정 (사용자 정의 스키마)
COLLECTION_TABLE = "public.ko_sbert_collection"
EMBEDDING_TABLE = "public.ko_sbert_embedding"
COLLECTION_NAME = "embedding_vector_ko_sbert" # 사용할 컬렉션 이름


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
# 3. [핵심] 사용자 정의 테이블 적재 함수 (Relational Insert)
# ------------------------------------------------------------------------------
def get_or_create_collection_id(cursor, collection_name):
    """
    컬렉션 테이블(ko_sbert_collection)에서 ID를 조회하거나 생성합니다.
    """
    # 1. 이미 존재하는지 확인
    select_sql = f"SELECT uuid FROM {COLLECTION_TABLE} WHERE name = %s"
    cursor.execute(select_sql, (collection_name,))
    result = cursor.fetchone()
    
    if result:
        return result[0] # 기존 ID 반환
    
    # 2. 없으면 새로 생성 (표준 UUID 필요)
    new_uuid = str(uuid.uuid4())
    # cmetadata 컬럼은 JSON 형식이므로 json.dumps 필요
    insert_sql = f"INSERT INTO {COLLECTION_TABLE} (uuid, name, cmetadata) VALUES (%s, %s, %s)"
    cursor.execute(insert_sql, (new_uuid, collection_name, json.dumps({"description": "Real Estate Data"})))
    
    return new_uuid

def save_to_specific_table(documents: List[Document], embeddings: Embeddings):
    """
    LangChain을 통하지 않고, psycopg2로 직접 데이터를 INSERT 합니다.
    """
    if not documents:
        return

    print(f"\n[System] 사용자 정의 테이블 적재 시작 (총 {len(documents)}건)")
    
    # 1. 텍스트 리스트 추출 및 일괄 벡터화 (속도 최적화)
    texts = [doc.page_content for doc in documents]
    try:
        print(" -> 벡터화 진행 중...")
        vectors = embeddings.embed_documents(texts)
    except Exception as e:
        print(f"[Error] 임베딩 변환 실패: {e}")
        return

    # 2. DB 연결 및 트랜잭션 처리
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, dbname=DB_NAME
        )
        cur = conn.cursor()

        # (1) 컬렉션 ID 확보 (Foreign Key 연결을 위해 필수)
        collection_id = get_or_create_collection_id(cur, COLLECTION_NAME)
        
        # (2) 임베딩 데이터 INSERT 쿼리
        # 스키마 순서: uuid, collection_id, embedding, document, cmetadata, custom_id
        insert_sql = f"""
            INSERT INTO {EMBEDDING_TABLE} (
                uuid, 
                collection_id, 
                embedding, 
                document, 
                cmetadata, 
                custom_id
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """

        success_count = 0
        for i, doc in enumerate(documents):
            # [UUID] DB의 PK용 표준 UUID 생성 (필수)
            row_uuid = str(uuid.uuid4())
            
            # [Vector]
            vector = vectors[i]
            
            # [Content]
            content = doc.page_content
            
            # [JSON] 파이썬 딕셔너리를 JSON 문자열로 변환 (필수)
            meta_json = json.dumps(doc.metadata)
            
            # [Custom ID] 파일에 있던 원래 ID (RENT_...)는 여기에 저장
            # 메타데이터에 'id' 또는 'rdb_id' 키가 있다고 가정
            custom_id = doc.metadata.get("rdb_id") or doc.metadata.get("id")
            if not custom_id:
                # 데이터에 ID가 없을 경우 임시 ID 생성
                custom_id = f"AUTO_{uuid.uuid4().hex[:8]}"

            # 쿼리 실행
            cur.execute(insert_sql, (
                row_uuid,
                collection_id,
                vector,
                content,
                meta_json,
                custom_id
            ))
            success_count += 1

        conn.commit()
        print(f"[Success] 총 {success_count}건 적재 완료.")
        print(f" -> Collection ID: {collection_id}")

    except Exception as e:
        print(f"[Error] DB 적재 중 오류 발생: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()
    

# ------------------------------------------------------------------------------
# 4. 자동화 로직 (안전장치 및 파일 순회 기능 추가)
# ------------------------------------------------------------------------------
def run_full_automation(embeddings: Embeddings):
    """
    여러 파일 패턴을 자동으로 탐색하고,
    DB 적재 함수를 호출하기 전에 '데이터 검증(안전장치)'을 수행합니다.
    """
    
    # 1. 처리할 파일 패턴 목록
    # 날짜 부분(202xxxxx)을 *로 처리하여 모든 날짜 파일을 인식함.
    target_patterns = [
        "apt_data_*.txt",        # 아파트 데이터
        "land_data_*.txt",       # 토지 데이터
        "bitkinds_news_*.txt",   # 뉴스 데이터
        "officetel_data_*.txt",  # 오피스텔 데이터
        "apt_rent_data_*.txt"    # 기존 전월세 데이터
    ]

    print(f"\n 다중 카테고리 데이터 적재 시작")

    total_files = 0

    for pattern in target_patterns:
        # 패턴에 맞는 파일 찾기 (날짜 무관, 정렬하여 순서대로 처리)
        files = sorted(glob.glob(pattern))
        
        if not files:
            continue

        print(f"\n [카테고리] '{pattern}' 패턴 파일 {len(files)}개 발견")
        
        for file_path in files:
            print(f"\n--- [Processing] {file_path} ---")
            try:
                # (1) 파일 로드 및 청킹
                raw_data = load_raw_jsonl_file(file_path)
                chunks = create_and_chunk_documents(raw_data)
                
                # (2) [안전장치] DB 함수 호출 전, ID 누락 검사 및 보정
                # DB 함수를 건드리지 않고, 들어가는 데이터(chunks)를 미리 수정합니다.
                fixed_count = 0
                for doc in chunks:
                    current_id = doc.metadata.get("rdb_id") or doc.metadata.get("id")
                    if not current_id:
                        # ID가 없으면 메타데이터에 'rdb_id'를 강제로 주입
                        doc.metadata["rdb_id"] = f"AUTO_{uuid.uuid4().hex[:8]}"
                        fixed_count += 1
                
                if fixed_count > 0:
                    print(f"   ⚠️ [Safety] ID가 없는 {fixed_count}개 데이터에 임시 ID를 발급했습니다.")

                # (3) DB 적재 함수 호출 (검증된 chunks 전달)
                save_to_specific_table(chunks, embeddings)
                
                total_files += 1
                
            except Exception as e:
                print(f"❌ [Error] {file_path} 처리 실패: {e}")
                continue

    print(f"\n✅ [완료] 총 {total_files}개의 파일 처리가 끝났습니다.")


# ------------------------------------------------------------------------------
# 5. 메인 실행부
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    # 1. 모델 로드
    model = load_embedding_model()

    # 2. 자동화 시스템 가동
    run_full_automation(model)
    
# ------------------------------------------------------------------------------
# 5. 메인 실행 (테스트)
# ------------------------------------------------------------------------------
# if __name__ == "__main__":
#     # 1. 모델 로드
#     ko_sbert_model = load_embedding_model()

#     # --- [Mode 1] 데이터 색인 ---
#     file_path = 'apt_rent_data_20260111.txt' # 파일명 확인

#     # # 2. 자동화 모드 실행
#     # run_batch_indexing(openai_model, pattern="apt_rent_data_*.txt")
    
#     if os.path.exists(file_path):
#         print("\n=== [Mode 1] 지식 기반 구축 (데이터 색인) ===")
#         # 청킹 모듈 호출
#         raw_texts = load_raw_jsonl_file(file_path)
#         final_chunks = create_and_chunk_documents(raw_texts)
        
#         # DB 저장 실행
#         save_to_specific_table(final_chunks, ko_sbert_model)
#     else:
#         print(f"\n[Info] 데이터 파일('{file_path}')이 없어 데이터 색인 과정을 건너뜁니다.")

#     # --- [Mode 2] 사용자 질문 벡터화 ---
#     print("\n=== [Mode 2] 사용자 질문 임베딩 테스트 ===")
#     test_question = "강남 반포자이 최근 6개월 전세 시세 알려줘."