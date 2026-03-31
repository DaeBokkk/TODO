import os
import sys
from psycopg_pool import ConnectionPool
from dotenv import load_dotenv

# 임베딩 모델 로딩과 분리하여 DB 연결 및 SQL 템플릿만 독립적으로 관리하기 위한 모듈
load_dotenv()

# ----------------------------------------------------
# 1. DB 환경 변수 검증 및 연결 정보 설정
# ----------------------------------------------------
REQUIRED_ENV_VARS = ["PG_HOST", "PG_PORT", "PG_USER", "PG_PASSWORD", "PG_DATABASE"]
missing_vars = [var for var in REQUIRED_ENV_VARS if not os.environ.get(var)]

# 환경 변수가 하나라도 누락될 경우 DB 연결이 불가능하므로 즉시 실행 중단
if missing_vars:
    cwd = os.getcwd()
    print(f"❌ 필수 환경 변수가 누락되었습니다: {missing_vars} (현재 디렉토리: {cwd})")
    sys.exit(1)

DB_URL_RAW = (
    f"host={os.environ['PG_HOST']} port={os.environ['PG_PORT']} "
    f"user={os.environ['PG_USER']} password={os.environ['PG_PASSWORD']} "
    f"dbname={os.environ['PG_DATABASE']}"
)

# ----------------------------------------------------
# 2. 하이브리드 검색 SQL 템플릿
# ----------------------------------------------------
# 공간 필터링(region_code) 및 시계열 필터링(contract_date)을 먼저 수행하여 검색 모수를 줄인 뒤,
# 벡터 유사도(70%)와 최신성 가중치(30%)를 결합하여 최종 리랭킹을 수행합니다.
HYBRID_SEARCH_SQL_TEMPLATE = """
SELECT
    t1.uuid::VARCHAR AS chunk_id,
    t1.document AS chunk_text,
    (t1.cmetadata ->> 'contract_date')::DATE AS enactment_date,
    -- 유사도 0.7 + 최신성 0.3 하이브리드 스코어링 (UT-09)
    ( (t1.embedding <-> %(query_vector)s::vector) * 0.7 ) + 
    ( (1.0 - public.rerank_context_freshness((t1.cmetadata ->> 'contract_date')::DATE)) * 0.3 ) AS total_score 
FROM
    public.{target_table} t1
WHERE
    -- 공간 필터링 (UT-06)
    (t1.cmetadata ->> 'region_code') = ANY(%(region_codes)s)
    
    -- 시계열 범위 최적화 및 인덱스 활용 (UT-07, UT-15)
    -- 데이터를 DATE로 명시적 캐스팅하여 검색 정확도 및 속도 향상
    AND (t1.cmetadata ->> 'contract_date')::DATE >= %(date_start)s::DATE
    AND (t1.cmetadata ->> 'contract_date')::DATE <= %(date_end)s::DATE
ORDER BY
    total_score ASC 
LIMIT %(k_limit)s;
"""

# ----------------------------------------------------
# 3. DB Connection Pool 관리
# ----------------------------------------------------
db_pool = None

def get_db_pool() -> ConnectionPool:
    """
    싱글톤 패턴으로 DB Connection Pool을 생성 및 반환합니다.
    제한된 자원 환경(1GB RAM)을 고려하여 네트워크 지연 및 유휴 상태 연결 끊김을 방지하도록 설정되었습니다.
    """
    global db_pool
    if db_pool is None:
        try:
            db_pool = ConnectionPool(
                conninfo=DB_URL_RAW,
                min_size=1,            # Cold Start 지연을 방지하기 위해 최소 1개의 연결은 항상 유지
                max_size=20,
                timeout=120.0,         # 리소스 부족으로 인한 쿼리 병목 발생 시 최대 2분간 대기 허용
                kwargs={
                    "connect_timeout": 30,  # 초기 DB 서버 접속 시도 시 여유 시간 확보
                    "keepalives": 1,        # 유휴 상태(Idle)에서 DB에 의해 연결이 강제 종료되는 것을 방지
                    "keepalives_idle": 30,
                    "keepalives_interval": 10,
                    "keepalives_count": 5
                }
            )
            print("✅ Raw DB Connection Pool 준비 완료.")
        except Exception as e:
            print(f"❌ DB Pool 생성 실패: {e}")
            sys.exit(1)
    return db_pool