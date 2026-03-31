import time
import uuid
import requests
from contextlib import asynccontextmanager
from typing import Optional

import psycopg
from psycopg_pool import ConnectionPool
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from dbforrag import get_db_pool, HYBRID_SEARCH_SQL_TEMPLATE
from core.embedding_loader import load_embedding_model

# ----------------------------------------------------
# 1. 전역 설정 및 인프라 최적화
# ----------------------------------------------------
db_pool: Optional[ConnectionPool] = None

session = requests.Session()
retry_strategy = Retry(
    total=3, 
    backoff_factor=1, 
    status_forcelist=[429, 500, 502, 503, 504]
)
session.mount("https://", HTTPAdapter(max_retries=retry_strategy))

@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool
    print("🚀 서버 시작: 리소스 초기화 및 DB 풀 생성 중...")
    try:
        db_pool = get_db_pool()
        print("✅ DB Connection Pool 준비 완료.")
    except Exception as e:
        print(f"❌ 초기화 실패: {e}")
        db_pool = None
    yield 
    if db_pool:
        db_pool.close()
        print("👋 서버 종료: DB Pool 반납 완료")

app = FastAPI(lifespan=lifespan)

# ----------------------------------------------------
# 2. 데이터 수신 모델 (팀장님 규격 반영)
# ----------------------------------------------------
class ParserInput(BaseModel):
    main_intent: str = Field(..., description="사용자의 질문 의도")
    location: str = Field(..., description="조회 지역")
    complex_name: Optional[str] = None   
    property_type: Optional[str] = None  
    price_metric: Optional[str] = None   
    start_date: Optional[str] = Field(None, description="조회 시작일 (YYYY-MM-DD)") 
    end_date: Optional[str] = Field(None, description="조회 종료일 (YYYY-MM-DD)")   
    embedding_model: str = "ko-sbert" 
    llm_model: str = "llama-3-8b"
    k: int = 5
    chat_id: int = 0

def make_search_text(input_data: ParserInput) -> str:
    """텍스트 정규화를 통해 임베딩 모델의 컨텍스트 이해도를 높입니다."""
    raw = [
        input_data.location, input_data.complex_name, input_data.property_type, 
        input_data.price_metric, input_data.main_intent, input_data.start_date, input_data.end_date
    ]
    valid = [str(c).strip() for c in raw if c and str(c).strip()]
    search_text = " ".join(valid)
    print(f"🔎 [Search Text]: {search_text}")
    return search_text

# ----------------------------------------------------
# 3. 핵심 통합 검색 로직 (Zero-Result Safeguard 포함)
# ----------------------------------------------------
def execute_search_logic(request: ParserInput):
    if db_pool is None:
        raise HTTPException(status_code=503, detail="DB Pool Not Ready")
    
    search_text = make_search_text(request)
    
    # 1. 모델 로드 (Lazy Loading 및 캐싱 적용)
    try:
        current_model = load_embedding_model(request.embedding_model)
        query_vector_raw = current_model.embed_query(search_text) 
    except Exception as e:
        print(f"❌ 임베딩 실패: {e}")
        raise HTTPException(status_code=500, detail="Embedding failed")
    
    MODEL_TABLE_MAP = {
        "ko-sbert": "ko_sbert_embedding", 
        "kcbert": "kcbert_embedding", 
        "openai-v3": "openai_embedding"
    }
    target_table = MODEL_TABLE_MAP.get(request.embedding_model, "ko_sbert_embedding")
    
    start_time = time.time()
    new_query_uuid = str(uuid.uuid4())

    try:
        with db_pool.connection() as conn:
            # 2. Zero-Result Safeguard (공간 필터링)
            search_term = request.location.replace(" ", "")
            
            with conn.cursor() as cur:
                # SQL 와일드카드 %와 psycopg 플레이스홀더 충돌 방지를 위해 || 연산자 사용
                cur.execute(
                    "SELECT REGION_CODE FROM REGION_INFO WHERE REPLACE(REGION_NAME, ' ', '') LIKE '%%' || %s || '%%';", 
                    (search_term,)
                )
                code_results = cur.fetchall()
                
                if not code_results:
                    raise HTTPException(status_code=404, detail=f"등록되지 않은 지역입니다: {request.location}")
                
                final_region_codes = [row[0] for row in code_results]

            # 3. 하이브리드 검색 (시계열 인덱스 최적화 반영)
            dynamic_sql = HYBRID_SEARCH_SQL_TEMPLATE.format(target_table=target_table)
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                params = {
                    'query_vector': str(query_vector_raw), 
                    'region_codes': final_region_codes, 
                    'date_start': request.start_date, 
                    'date_end': request.end_date, 
                    'k_limit': request.k
                }
                cur.execute(dynamic_sql, params)
                db_results = cur.fetchall()
                
                latency = float((time.time() - start_time) * 1000)
                print(f"✅ 검색 완료: {new_query_uuid} ({latency:.2f}ms)")
                
                return {"query_id": new_query_uuid, "rows": db_results}

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ DB 검색 처리 에러: {e}")
        raise HTTPException(status_code=500, detail="Database search failed")

# ----------------------------------------------------
# 4. API 엔드포인트
# ----------------------------------------------------
@app.post("/hybrid_search")
def run_hybrid_search(request: ParserInput):
    search_data = execute_search_logic(request)
    formatted_contexts = []
    
    for row in search_data["rows"]:
        # 표준 컨텍스트 포맷 (UT-12 준수)
        date_info = row.get('enactment_date', '날짜미상')
        formatted_str = f"[작성일: {date_info}] | [{request.location}] | {row.get('chunk_text')}"
        formatted_contexts.append(formatted_str)
        
    return {"query_id": search_data["query_id"], "contexts": formatted_contexts}

@app.get("/status")
async def get_status():
    return {"status": "online", "database": "connected" if db_pool else "disconnected"}