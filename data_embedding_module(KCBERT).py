import os
import time
import glob  # 폴더 내 여러 파일을 찾기 위한 라이브러리
import uuid
import json
from typing import List
import psycopg2 # 사용자가 설치한 psycopg2-binary 사용
from datetime import datetime  # 오늘 날짜를 구하기 위한 모듈

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

# (1) 임베딩 모델 설정 (KCBERT로 변경됨)
MODEL_ID = "beomi/kcbert-base"
MODEL_DEVICE = "cpu"

# (2) PostgreSQL DB 연결 정보 (Ngrok 정보 반영)
DB_HOST = "3.39.23.25"
DB_PORT = "5432"  # 포트 번호 확인 필요
DB_USER = "rag"
DB_PASSWORD = "rag"
DB_NAME = "rag"

# [중요] LangChain PGVector는 SQLAlchemy 형식의 연결 문자열을 사용한다.
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
    KCBERT 모델을 메모리에 로드합니다.
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
# 3. [핵심] 사용자 정의 테이블 배치(Batch) 적재 함수
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

def save_to_specific_table(documents: List[Document], embeddings: Embeddings, batch_size: int = 100):
    """
    LangChain을 통하지 않고, psycopg2로 직접 데이터를 INSERT 합니다.
    OOM 방지 및 DB 부하 감소를 위해 지정된 배치 사이즈(100건) 단위로 처리합니다.
    """
    if not documents:
        return

    print(f"\n[System] 사용자 정의 테이블 적재 시작 (총 {len(documents)}건, 배치 사이즈: {batch_size}건)")
    
    # 1. DB 연결 및 트랜잭션 처리
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, dbname=DB_NAME
        )
        cur = conn.cursor()

        # (1) 컬렉션 ID 확보 (Foreign Key 연결을 위해 필수)
        collection_id = get_or_create_collection_id(cur, COLLECTION_NAME)
        
        # (2) 임베딩 데이터 INSERT 쿼리 템플릿
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
        
        # 2. 데이터를 배치 사이즈(100건)만큼 쪼개어 처리
        for i in range(0, len(documents), batch_size):
            batch_docs = documents[i : i + batch_size]
            current_batch_len = len(batch_docs)
            
            # [Step 1] 배치 단위로 텍스트 추출 및 벡터화 진행 (메모리 절약)
            texts = [doc.page_content for doc in batch_docs]
            print(f" -> [{i+1} ~ {i+current_batch_len}] 임베딩 변환 중...")
            
            try:
                vectors = embeddings.embed_documents(texts)
            except Exception as e:
                print(f"[Error] {i+1}번째 배치 임베딩 변환 실패: {e}")
                continue

            # [Step 2] DB 적재를 위한 튜플 데이터 생성
            insert_data = []
            for j, doc in enumerate(batch_docs):
                row_uuid = str(uuid.uuid4())
                vector = vectors[j]
                content = doc.page_content
                meta_json = json.dumps(doc.metadata)
                
                custom_id = doc.metadata.get("rdb_id") or doc.metadata.get("id")
                if not custom_id:
                    custom_id = f"AUTO_{uuid.uuid4().hex[:8]}"

                insert_data.append((
                    row_uuid,
                    collection_id,
                    vector,
                    content,
                    meta_json,
                    custom_id
                ))

            # [Step 3] executemany를 활용해 DB에 한 번에 다중 INSERT 실행
            cur.executemany(insert_sql, insert_data)
            conn.commit()  # 각 배치마다 커밋하여 안전하게 저장
            
            success_count += current_batch_len
            print(f" -> [{success_count}/{len(documents)}] DB 배치 적재 완료.")

        print(f"\n[Success] 총 {success_count}건 배치 적재 프로세스 완료.")
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
# 4. [신규] 금일 날짜 기준 자동화 로직
# ------------------------------------------------------------------------------
def run_full_automation(embeddings: Embeddings):
    """
    오늘 날짜(YYYYMMDD)에 해당하는 파일 패턴만 자동으로 탐색하고,
    DB 적재 함수를 호출하기 전에 '데이터 검증(안전장치)'을 수행해요.
    """
    
    # 프로그램이 실행되는 '오늘' 날짜를 YYYYMMDD 형태로 가져온다.
    # today_str = datetime.now().strftime("%Y%m%d")
    today_str = 'apt_data_20260131'

    target_patterns = [
        f"**/**/*{today_str}*.txt"
    ]

    print(f"\n🚀 [Automation] 금일({today_str}) 데이터 적재 시작")

    total_files = 0

    for pattern in target_patterns:
        # 패턴에 맞는 파일 찾기 (오늘 날짜 파일만 검색돼요)
        files = sorted(glob.glob(pattern))
        
        if not files:
            continue

        print(f"\n📂 [카테고리] '{pattern}' 패턴 파일 {len(files)}개 발견")
        
        for file_path in files:
            print(f"\n--- [Processing] {file_path} ---")
            try:
                # (1) 파일 로드 및 청킹
                raw_data = load_raw_jsonl_file(file_path)
                chunks = create_and_chunk_documents(raw_data)
                
                # (2) [안전장치] DB 함수 호출 전, ID 누락 검사 및 보정
                fixed_count = 0
                for doc in chunks:
                    current_id = doc.metadata.get("rdb_id") or doc.metadata.get("id")
                    if not current_id:
                        # ID가 없으면 메타데이터에 'rdb_id'를 강제로 주입
                        doc.metadata["rdb_id"] = f"AUTO_{uuid.uuid4().hex[:8]}"
                        fixed_count += 1
                
                if fixed_count > 0:
                    print(f"   ⚠️ [Safety] ID가 없는 {fixed_count}개 데이터에 임시 ID를 발급했어요.")

                # (3) DB 적재 함수 호출 (검증된 chunks 전달, 100건 단위 배치 처리 적용됨)
                save_to_specific_table(chunks, embeddings, batch_size=100)
                
                total_files += 1
                
            except Exception as e:
                print(f"❌ [Error] {file_path} 처리 실패: {e}")
                continue

    print(f"\n✅ [완료] 총 {total_files}개의 금일 파일 처리가 끝났어요.")


# ------------------------------------------------------------------------------
# 5. 메인 실행부
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    # 1. 모델 로드
    model = load_embedding_model()

    # 2. 자동화 시스템 가동
    run_full_automation(model)