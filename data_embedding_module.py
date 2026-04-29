import os
import time
import glob  
import uuid
import json
import dotenv
from typing import List
import psycopg2 
from datetime import datetime 

# LangChain 필수 라이브러리
from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings
from langchain_openai import OpenAIEmbeddings 
from langchain_community.vectorstores import PGVector

# 청킹 모듈 연동
try:
    from data_chunking_module import load_raw_jsonl_file, create_and_chunk_documents
except ImportError:
    pass

dotenv.load_dotenv()
OPENAI_API_KEY =  os.getenv('Emb_KEY')

# ------------------------------------------------------------------------------
# 1. [설계 설정] 임베딩 모델 및 DB 연결 설정
# ------------------------------------------------------------------------------
MODEL_ID = "text-embedding-3-small"


DB_HOST = "3.39.23.25"
DB_PORT = "5432"  
DB_USER = "rag"
DB_PASSWORD = "rag"
DB_NAME = "rag"

COLLECTION_TABLE = "public.ko_sbert_collection"
EMBEDDING_TABLE = "public.ko_sbert_embedding"
COLLECTION_NAME = "embedding_vector_ko_sbert" 

# ------------------------------------------------------------------------------
# 2. 임베딩 모델 로드 함수
# ------------------------------------------------------------------------------
def load_embedding_model() -> Embeddings:
    print(f"\n[System] 임베딩 모델 로딩 중... ({MODEL_ID})")
    start = time.time()
    
    embeddings = OpenAIEmbeddings(
        model=MODEL_ID,
        openai_api_key=OPENAI_API_KEY,
        chunk_size=100  
    )
    
    print(f"[System] 로딩 완료 ({time.time() - start:.2f}초)")
    return embeddings

# ------------------------------------------------------------------------------
# 3. [핵심] 사용자 정의 테이블 적재 함수 (진행률 출력 기능 추가)
# ------------------------------------------------------------------------------
def get_or_create_collection_id(cursor, collection_name):
    select_sql = f"SELECT uuid FROM {COLLECTION_TABLE} WHERE name = %s"
    cursor.execute(select_sql, (collection_name,))
    result = cursor.fetchone()
    
    if result:
        return result[0] 
    
    new_uuid = str(uuid.uuid4())
    insert_sql = f"INSERT INTO {COLLECTION_TABLE} (uuid, name, cmetadata) VALUES (%s, %s, %s)"
    cursor.execute(insert_sql, (new_uuid, collection_name, json.dumps({"description": "Real Estate Data"})))
    
    return new_uuid

def save_to_specific_table(documents: List[Document], embeddings: Embeddings):
    if not documents:
        return

    total_docs = len(documents)
    print(f"\n[System] 사용자 정의 테이블 적재 시작 (총 {total_docs}건)")
    
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, dbname=DB_NAME
        )
        cur = conn.cursor()

        collection_id = get_or_create_collection_id(cur, COLLECTION_NAME)
        
        insert_sql = f"""
            INSERT INTO {EMBEDDING_TABLE} (
                uuid, collection_id, embedding, document, cmetadata, custom_id
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """

        # --- [수정된 부분] 100개 단위로 끊어서 진행 상황을 실시간 출력합니다 ---
        batch_size = 100
        success_count = 0
        
        print(" -> 벡터화 및 DB 저장 진행 중... (100개 단위 출력)")
        
        for i in range(0, total_docs, batch_size):
            # 1. 100개 문서 가져오기
            batch_docs = documents[i : i + batch_size]
            batch_texts = [doc.page_content for doc in batch_docs]
            
            # 2. 100개만 벡터화 요청 (여기서 약간 시간이 걸립니다)
            batch_vectors = embeddings.embed_documents(batch_texts)
            
            # 3. 100개 DB 적재
            for j, doc in enumerate(batch_docs):
                row_uuid = str(uuid.uuid4())
                vector = batch_vectors[j]
                content = doc.page_content
                meta_json = json.dumps(doc.metadata)
                
                custom_id = doc.metadata.get("rdb_id") or doc.metadata.get("id")
                if not custom_id:
                    custom_id = f"AUTO_{uuid.uuid4().hex[:8]}"

                cur.execute(insert_sql, (
                    row_uuid, collection_id, vector, content, meta_json, custom_id
                ))
                success_count += 1
            
            # 100개 단위로 DB에 확정(Commit)
            conn.commit()
            
            # 4. 실시간 진행 상황 출력
            print(f"   진행 상황: {success_count} / {total_docs} 건 완료")

        print(f"\n [Success] 총 {success_count}건 적재 완료.")
        print(f" -> Collection ID: {collection_id}")

    except Exception as e:
        print(f" [Error] DB 적재 중 오류 발생: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()
    
# ------------------------------------------------------------------------------
# 4. 자동화 로직
# ------------------------------------------------------------------------------
def run_full_automation(embeddings: Embeddings):
    
    today_str = datetime.now().strftime("%Y%m%d")
    
    target_patterns = [f"**/**/*{today_str}*.txt"]

    print(f"\n [Automation] 금일({today_str}) 데이터 적재 시작")
    total_files = 0

    for pattern in target_patterns:
        files = sorted(glob.glob(pattern))
        if not files: continue

        print(f"\n [카테고리] '{pattern}' 패턴 파일 {len(files)}개 발견")
        
        for file_path in files:
            print(f"\n--- [Processing] {file_path} ---")
            try:
                raw_data = load_raw_jsonl_file(file_path)
                chunks = create_and_chunk_documents(raw_data)
                
                fixed_count = 0
                for doc in chunks:
                    current_id = doc.metadata.get("rdb_id") or doc.metadata.get("id")
                    if not current_id:
                        doc.metadata["rdb_id"] = f"AUTO_{uuid.uuid4().hex[:8]}"
                        fixed_count += 1
                
                if fixed_count > 0:
                    print(f"    [Safety] ID가 없는 {fixed_count}개 데이터에 임시 ID 발급 완료.")

                save_to_specific_table(chunks, embeddings)
                total_files += 1
                
            except Exception as e:
                print(f" [Error] {file_path} 처리 실패: {e}")
                continue

    print(f"\n [완료] 총 {total_files}개의 금일 파일 처리가 끝났어요.")

# ------------------------------------------------------------------------------
# 5. 메인 실행부
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    model = load_embedding_model()
    run_full_automation(model)