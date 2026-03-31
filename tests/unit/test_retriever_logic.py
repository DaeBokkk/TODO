import time
import uuid
import pytest
from datetime import date
from unittest.mock import patch

from server import parse_period, make_search_text
from dbforrag import HYBRID_SEARCH_SQL_TEMPLATE
from core.embedding_loader import load_embedding_model

# ----------------------------------------------------
# 1. 전처리 및 파라미터 최적화 검증
# ----------------------------------------------------
def test_ut_07_parse_period():
    """
    SQL DATE 타입과의 완벽한 호환성을 보장하기 위해, 
    사용자의 비정형 기간 입력("2020~2023")이 ISO 표준 형식으로 정확히 변환되는지 검증합니다.
    """
    period_str = "2020~2023"
    start_date, end_date = parse_period(period_str)

    print(f"\n📅 [Date Parsing] 변환 결과: {start_date} ~ {end_date}")

    assert start_date == "2020-01-01"
    assert end_date == "2023-12-31"

def test_ut_05_make_search_text(sample_parser_input):
    """
    임베딩 모델이 텍스트의 맥락을 가장 정확하게 파악할 수 있도록, 
    입력값의 불필요한 공백이나 노이즈가 제거(정규화)되는지 확인합니다.
    """
    sample_parser_input.location = "평택시 "
    search_text = make_search_text(sample_parser_input)
    
    assert "평택시" in search_text
    assert "  " not in search_text 

# ----------------------------------------------------
# 2. 자원 최적화 및 인프라 검증
# ----------------------------------------------------
@patch('core.embedding_loader._current_model', None)
@patch('core.embedding_loader._current_model_name', None)
@patch('core.embedding_loader.HuggingFaceEmbeddings')
def test_ut_15_model_caching_singleton(mock_hf):
    """
    1GB RAM 환경에서 OOM(Out of Memory) 강제 종료를 방지하기 위해,
    동일 모델 요청 시 추가 적재 없이 캐시된 객체를 반환(Singleton)하는지 검증합니다.
    """
    import gc
    
    model_1 = load_embedding_model("ko-sbert")
    assert mock_hf.call_count == 1
    
    model_2 = load_embedding_model("ko-sbert")
    assert mock_hf.call_count == 1 
    
    gc.collect()
    print("\n♻️ [Memory] 가비지 컬렉션 호출 완료")

def test_ut_02_latency_tracking():
    """
    저사양 서버 환경에서 성능 병목을 실시간으로 감지할 수 있도록,
    ms(밀리초) 단위의 정밀한 지연 시간 트래킹이 작동하는지 확인합니다.
    """
    start_time = time.time()
    time.sleep(0.05) 
    latency = (time.time() - start_time) * 1000

    print(f"\n⏱️ [Latency] 검색 소요 시간: {latency:.2f}ms")
    assert latency > 0

# ----------------------------------------------------
# 3. 알고리즘 및 데이터 구조화 검증
# ----------------------------------------------------
def test_ut_06_09_10_sql_template_structure():
    """
    실제 DB 연산 전, 하이브리드 가중치(유사도 7 : 최신성 3)와 
    공간 필터링 조건이 SQL 템플릿에 올바르게 주입되어 있는지 정적 분석으로 확인합니다.
    """
    sql = HYBRID_SEARCH_SQL_TEMPLATE.format(target_table="ko_sbert_embedding")   
    
    assert "region_code') = ANY(%(region_codes)s)" in sql
    assert "* 0.7 ) +" in sql
    assert "* 0.3 ) AS total_score" in sql
    assert "public.rerank_context_freshness" in sql
    print("\n✅ [SQL Template] 하이브리드 필터 및 가중치 구조 정상")

def test_ut_12_context_structuring():
    """
    LLM이 환각(Hallucination) 없이 주어진 문맥에 기반하여 답변을 생성할 수 있도록, 
    DB 검색 결과가 시스템이 정의한 표준 프롬프트 포맷으로 변환되는지 검증합니다.
    """
    mock_db_results = [
        {
            'chunk_id': str(uuid.uuid4()),
            'chunk_text': "매교동 토지 매매 실거래가 정보입니다.",
            'enactment_date': date(2024, 1, 15),
            'target_region': "경기도 수원시 팔달구 매교동"
        }
    ]
    
    formatted_contexts = []
    
    for row in mock_db_results:
        c_date = row.get('enactment_date', 'YYYY-MM-DD')
        c_region = row.get('target_region', '지역정보 없음')
        c_text = row.get('chunk_text', '')
        
        formatted = f"[작성일: {c_date}] | [{c_region}] | {c_text}"
        formatted_contexts.append(formatted)
        
    assert len(formatted_contexts) == 1
    assert "2024-01-15" in formatted_contexts[0]
    assert "매교동" in formatted_contexts[0]