# dbforrag.py (최종 수정본)

import os
import sys
from typing import Optional
import psycopg 
from psycopg_pool import ConnectionPool
from dotenv import load_dotenv

# [중요] 임베딩 모델 로딩(HuggingFaceEmbeddings) 코드는 모두 삭제되었습니다.
# 이 파일은 오직 'DB 연결'과 'SQL 템플릿'만 관리합니다.

# ----------------------------------------------------
# 1. DB 연결 정보 설정
# ----------------------------------------------------
# 1. .env 파일 로드 (같은 폴더에 있는 .env를 찾아서 읽음)
load_dotenv()

# 2. 환경변수에서 값 가져오기 (비밀번호 노출 X)
# os.environ.get("변수명")을 쓰면 .env 내용을 가져옵니다.
pg_host = os.environ.get("PG_HOST")
pg_port = os.environ.get("PG_PORT")
pg_user = os.environ.get("PG_USER")
pg_password = os.environ.get("PG_PASSWORD")
pg_database = os.environ.get("PG_DATABASE")

# 값 확인용 (혹시 못 읽어오면 에러 내기 위함)
if not pg_host:
    print("❌ .env 파일을 찾을 수 없거나 내용이 비어있습니다.")
    sys.exit(1)

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