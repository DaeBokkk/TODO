# server.py

from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
from pydantic import BaseModel
from typing import List, Optional, Dict, Any # [수정] Dict, Any 추가됨
import time
import uuid
import psycopg 
from psycopg_pool import ConnectionPool
from datetime import date 

# [필수] dbforrag.py와 core 폴더가 있어야 합니다.
from dbforrag import get_db_pool, HYBRID_SEARCH_SQL_TEMPLATE
from core.embedding_loader import load_embedding_model

# ----------------------------------------------------
# 1. 전역 변수 설정
# ----------------------------------------------------
db_pool: Optional[ConnectionPool] = None 
embeddings = load_embedding_model() # 서버 시작 시 모델 로드

# ----------------------------------------------------
# 2. Lifespan (DB Pool 관리)
# ----------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool
    print("🚀 서버 시작 중...")
    try:
        db_pool = get_db_pool()
    except SystemExit:
        print("❌ DB 연결 실패")
        db_pool = None
    yield 
    if db_pool:
        db_pool.close()
        print("👋 DB Pool 정리 완료")

app = FastAPI(lifespan=lifespan)

# ----------------------------------------------------
# 3. 데이터 모델 정의
# ----------------------------------------------------
class ParserInput(BaseModel):
    """
    파서 팀원이 보내주는 JSON 구조입니다.
    """
    main_intent: str             # 질문 의도
    location: str                # 위치
    complex_name: Optional[str] = None  # 아파트 단지명
    property_type: Optional[str] = None # 부동산 속성
    price_metric: Optional[str] = None  # 가격 기준
    period: Optional[str] = None        # 조회 기간
    
    k: int = 5                   
    user_id: str = "parser_bot"
    chat_id: int = 0
    
# 디버깅용 상세 응답 모델
class SearchResponseDebug(BaseModel):
    content: str
    metadata: Dict[str, Any]
    score: float

# ----------------------------------------------------
# 4. Helper 함수 & 핵심 검색 로직
# ----------------------------------------------------
def parse_period(period_str: Optional[str]):
    default_start = "2020-01-01"
    default_end = str(date.today())

    if not period_str:
        return default_start, default_end
    
    try:
        if "~" in period_str:
            start, end = period_str.split("~")
            return start.strip(), end.strip()
        else:
            return period_str.strip(), default_end
    except:
        return default_start, default_end

def make_search_text(input_data: ParserInput) -> str:
    # 텍스트 필드를 모두 합쳐서 임베딩 문장 생성
    components = [
        input_data.location,
        input_data.complex_name,
        input_data.property_type,
        input_data.price_metric,
        input_data.main_intent,
        input_data.period
    ]
    return " ".join([c for c in components if c])

# [중요] 검색 로직을 함수로 분리 (두 엔드포인트에서 공통 사용)
def execute_search_logic(request: ParserInput):
    if db_pool is None:
        raise HTTPException(status_code=503, detail="DB Pool Not Ready")
    
    # 1. 임베딩할 텍스트 생성 & 벡터 변환
    search_text = make_search_text(request)
    query_vector_raw = embeddings.embed_query(search_text)
    
    # 2. 날짜 필터 준비
    start_date, end_date = parse_period(request.period)
    
    # 3. DB 검색 실행
    db_results = []
    conn = None
    
    start_time = time.time() # 로깅용 시간 측정 시작

    try:
        with db_pool.connection() as conn:
            # [Step A] 지역명 -> 지역코드 조회
            region_wildcard = '%' + request.location + '%'
            lookup_sql = "SELECT REGION_CODE FROM REGION_INFO WHERE REGION_NAME LIKE %s LIMIT 1;" 
            
            final_region_code = None
            with conn.cursor() as cur:
                cur.execute(lookup_sql, (region_wildcard,))
                code_result = cur.fetchone()
                if not code_result:
                    raise HTTPException(status_code=404, detail=f"Unknown location: {request.location}")
                final_region_code = code_result[0]

            # [Step B] 하이브리드 검색
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                params = {
                    'query_vector': str(query_vector_raw),
                    'region_code': final_region_code, 
                    'date_start': start_date,
                    'date_end': end_date, 
                    'k_limit': request.k
                }
                
                # 서버 터미널 디버깅 로그
                print(f"🔍 [Search] Query: '{search_text}'")
                print(f"   Filter: Region({final_region_code}), Date({start_date}~{end_date})")

                cur.execute(HYBRID_SEARCH_SQL_TEMPLATE, params)
                db_results = cur.fetchall()
                
                # [Step C] DB 내 로깅 함수 호출 (옵션)
                try:
                    query_id = str(uuid.uuid4())
                    latency = (time.time() - start_time) * 1000 
                    conn.execute(
                        "SELECT log_query_latency(%s, %s, %s, %s, %s)",
                        (query_id, request.user_id, request.chat_id, latency, True)
                    )
                    conn.commit()
                except Exception as e:
                    print(f"⚠️ 로깅 실패 (무시): {e}")

    except HTTPException:
        raise
    except Exception as e:
        if conn:
            try: conn.rollback()
            except: pass
        print(f"❌ DB Error: {e}")
        raise HTTPException(status_code=500, detail="Database error occurred")
        
    return db_results

# ----------------------------------------------------
# 5. 엔드포인트 정의
# ----------------------------------------------------

# [팀원용] 실제 서비스 주소 (문자열 리스트 반환)
@app.post("/hybrid_search", response_model=List[str])
def run_hybrid_search(request: ParserInput):
    rows = execute_search_logic(request)
    # 텍스트만 쏙 뽑아서 반환
    return [row['chunk_text'] for row in rows]

# [개발자용] 테스트 & 디버깅 주소 (상세 정보 반환)
@app.post("/hybrid_search_debug", response_model=List[SearchResponseDebug])
def run_hybrid_search_debug(request: ParserInput):
    rows = execute_search_logic(request)
    
    # 메타데이터와 점수까지 포함해서 반환
    results = []
    for row in rows:
        results.append({
            "content": row['chunk_text'],
            "metadata": {
                "source_id": row['chunk_id'],
                "contract_date": str(row['enactment_date']),
                "document_type": row['document_type'],
                "region_name": row['region_name']
            },
            "score": row['similarity_score']
        })
    return results

# 터미널 실행 명령어:
# uvicorn server:app --host 0.0.0.0 --port 8000 --reload