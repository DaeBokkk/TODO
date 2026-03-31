from fastapi.testclient import TestClient
from unittest.mock import patch
from server import app

client = TestClient(app)

@patch("server.execute_search_logic")
def test_hybrid_search_debug_endpoint(mock_search_logic, sample_parser_input):
    """
    /hybrid_search_debug 엔드포인트 테스트
    DB 로직은 Mocking하여 API 계층만 검증
    """
    # 1. 가짜 DB 결과 정의 (평택 데이터로 가정)
    mock_search_logic.return_value = [
        {
            "chunk_id": "test-uuid",
            "chunk_text": "평택 고덕신도시 아파트 매매 계약서...",
            "enactment_date": "2025-01-01",
            "document_type": "contract",
            "region_name": "평택",  # 여기는 평택인데
            "similarity_score": 0.95
        }
    ]

    # 2. API 호출
    response = client.post(
        "/hybrid_search_debug",
        json=sample_parser_input.model_dump()
    )

    # 3. 검증
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 1
    # [수정] 서울 -> 평택으로 변경
    assert data[0]["metadata"]["region_name"] == "평택"