# dbforrag.py (최종 수정본)

import os
import sys
from typing import Optional
import psycopg 
from psycopg_pool import ConnectionPool

# [중요] 임베딩 모델 로딩(HuggingFaceEmbeddings) 코드는 모두 삭제되었습니다.
# 이 파일은 오직 'DB 연결'과 'SQL 템플릿'만 관리합니다.

# ----------------------------------------------------
# 1. DB 연결 정보 설정
# ----------------------------------------------------
os.environ.setdefault("PG_HOST", "0.tcp.jp.ngrok.io")
os.environ.setdefault("PG_PORT", "15178")
os.environ.setdefault("PG_USER", "rag")
os.environ.setdefault("PG_PASSWORD", "rag")
os.environ.setdefault("PG_DATABASE", "rag")

DB_URL_RAW = (
    f"host={os.environ['PG_HOST']} port={os.environ['PG_PORT']} "
    f"user={os.environ['PG_USER']} password={os.environ['PG_PASSWORD']} "
    f"dbname={os.environ['PG_DATABASE']}"
)

# ----------------------------------------------------
# 2. 핵심 SQL 템플릿 정의 (date_end 포함 필수!)
# ----------------------------------------------------
HYBRID_SEARCH_SQL_TEMPLATE = """
SELECT
    t1.uuid::VARCHAR AS chunk_id,
    t1.document AS chunk_text,
    (t1.cmetadata ->> 'contract_date')::DATE AS enactment_date,
    t1.cmetadata ->> 'rdb_id' AS document_type,
    r2.REGION_NAME AS region_name,
    t1.embedding <-> %(query_vector)s::vector AS similarity_score 
FROM
    langchain_pg_embedding t1
JOIN 
    REGION_INFO r2 ON r2.REGION_CODE = (t1.cmetadata ->> 'region_code')
WHERE
    (t1.cmetadata ->> 'region_code') = %(region_code)s
    AND (t1.cmetadata ->> 'contract_date') >= %(date_start)s
    AND (t1.cmetadata ->> 'contract_date') <= %(date_end)s
ORDER BY
    similarity_score
LIMIT %(k_limit)s;
"""

# ----------------------------------------------------
# 3. DB Connection Pool 생성 함수
# ----------------------------------------------------
db_pool = None

def get_db_pool() -> ConnectionPool:
    global db_pool
    if db_pool is None:
        try:
            db_pool = ConnectionPool(
                conninfo=DB_URL_RAW,
                min_size=0,        
                max_size=20,       
                timeout=60,
                kwargs={
                    "connect_timeout": 10
                }          
            )
            print("✅ Raw DB Connection Pool (psycopg) 준비 완료.")
        except Exception as e:
            print(f"❌ DB Pool 생성 실패: {e}")
            sys.exit(1)
    return db_pool