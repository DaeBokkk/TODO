import os
import sys
import pytest
from dotenv import load_dotenv
from unittest.mock import MagicMock

# ----------------------------------------------------
# 1. 테스트 환경 경로 및 환경 변수 초기화
# ----------------------------------------------------
# pytest 실행 위치(작업 디렉토리)와 무관하게 프로젝트 루트의 모듈(server, core 등)을
# 정상적으로 import 하기 위해 sys.path 최상단에 프로젝트 절대 경로를 주입합니다.
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# 통합 테스트 시 필요한 DB 접속 정보 및 API 키를 로드하기 위해 .env를 명시적으로 주입합니다.
env_path = os.path.join(PROJECT_ROOT, ".env")
if os.path.exists(env_path):
    load_dotenv(env_path)
    print(f"✅ [System] 환경 변수 로드 완료: {env_path}")
else:
    print(f"⚠️ [Warning] .env 파일을 찾을 수 없습니다: {env_path}")

# 환경 변수와 경로가 모두 준비된 후 애플리케이션 모듈을 지연 임포트합니다.
try:
    from server import ParserInput
except ImportError as e:
    raise RuntimeError(f"❌ 모듈 임포트 실패 (경로 설정 오류): {e}\n현재 sys.path: {sys.path}")

# ----------------------------------------------------
# 2. Pytest Fixtures (테스트용 Mock 데이터)
# ----------------------------------------------------
@pytest.fixture
def sample_parser_input():
    """
    하이브리드 검색 및 텍스트 정규화 로직 테스트를 위한 표준 입력 페이로드입니다.
    """
    return ParserInput(
        main_intent="아파트 매매",
        location="강남구",
        complex_name="은마아파트",
        period="2020~2023",
        k=5,
        user_id="test_user",
        chat_id=123
    )

@pytest.fixture
def mock_embedding_model():
    """
    DB 검색 로직(UT-09 등)을 테스트할 때, 무거운 임베딩 모델(ko-sbert 등) 로드로 인한
    1GB RAM 초과(OOM) 방지 및 외부 API 호출(과금)을 차단하기 위한 더미 벡터 생성 객체입니다.
    """
    mock = MagicMock()
    # 768차원의 가짜 벡터 배열을 반환하도록 Mocking 설정
    mock.embed_query.return_value = [0.1] * 768
    return mock