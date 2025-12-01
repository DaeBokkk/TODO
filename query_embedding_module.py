import time
import psycopg  # pip install psycopg[binary]
from typing import Dict, Any, List

# ------------------------------------------------------------------------------
# 1. 모듈 및 환경 설정
# ------------------------------------------------------------------------------

try:
    from data_embedding_module import load_embedding_model
except ImportError:
    # 모듈이 없을 경우를 대비한 더미 로더
    from langchain_community.embeddings import HuggingFaceEmbeddings
    def load_embedding_model():
        return HuggingFaceEmbeddings(
            model_name="jhgan/ko-sbert-nli",
            model_kwargs={'device': 'cpu'},
            encode_kwargs={'normalize_embeddings': True}
        )

# DB 연결 정보
DB_INFO = {
    "host": "0.tcp.jp.ngrok.io",
    "port": "17339",
    "user": "rag",
    "password": "rag",
    "dbname": "rag"
}
DB_CONN_STRING = f"host={DB_INFO['host']} port={DB_INFO['port']} user={DB_INFO['user']} password={DB_INFO['password']} dbname={DB_INFO['dbname']}"

# ------------------------------------------------------------------------------
# 2. 쿼리 처리 메인 클래스
# ------------------------------------------------------------------------------
class StandaloneQueryProcessor:
    def __init__(self):
        print("🔄 [Init] 쿼리 프로세서 초기화 중...")
        self.embedding_model = load_embedding_model()
        print("✅ [Init] 초기화 완료.")

    # [삭제됨] _simple_extract 함수는 이제 필요 없습니다.

    def _convert_fields_to_text(self, fields: Dict[str, Any]) -> str:
        """
        딕셔너리 형태의 필드를 임베딩용 자연어 문장으로 변환합니다.
        """
        parts = []
        if fields.get("location"): parts.append(f"지역: {fields['location']}")
        if fields.get("complex_name"): parts.append(f"단지명: {fields['complex_name']}")
        if fields.get("property_type"): parts.append(f"유형: {fields['property_type']}")
        if fields.get("price_metric"): parts.append(f"가격기준: {fields['price_metric']}")
        if fields.get("main_intent"): parts.append(f"의도: {fields['main_intent']}")
        
        return ", ".join(parts) if parts else "부동산 정보 검색"

    # [핵심 수정] 인자명을 user_query(str) -> parsed_query(Dict)로 변경
    def process_and_save(self, parsed_query: Dict[str, Any], user_id: str = "anonymous") -> Dict[str, Any]:
        """
        서버에서 파싱된 쿼리(Dict)를 받아 -> 텍스트 변환 -> 임베딩 -> DB 저장을 수행합니다.
        """
        print(f"\n🚀 [Step 1] 파싱된 쿼리 수신: {parsed_query}")
        
        # 1. 임베딩을 위한 텍스트 변환
        search_text = self._convert_fields_to_text(parsed_query)
        print(f"   -> 변환된 검색 텍스트: '{search_text}'")
        
        # 2. 벡터 생성
        try:
            query_vector = self.embedding_model.embed_query(search_text)
            print(f"   -> 벡터 생성 완료 (차원: {len(query_vector)})")
        except Exception as e:
            print(f"❌ [Error] 임베딩 실패: {e}")
            return {}

        # 3. DB에 저장
        # 원본 질문 텍스트가 따로 없다면 변환된 텍스트를 저장합니다.
        self._insert_query_log(user_id, search_text, parsed_query, query_vector)
        
        return {
            "vector": query_vector,
            "search_text": search_text,
            "parsed_fields": parsed_query
        }

    def _insert_query_log(self, user_id: str, query: str, fields: Dict, vector: List[float]):
        """
        user_query 테이블에 저장
        """
        insert_sql = """
            INSERT INTO user_query (
                USER_ID, RAW_QUERY, 
                INTENT, LOCATION, COMPLEX_NAME, PROPERTY_TYPE, PRICE_METRIC, PERIOD,
                QUERY_VECTOR, CREATED_AT
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """
        
        params = (
            user_id, query,
            fields.get("main_intent"), fields.get("location"), fields.get("complex_name"),
            fields.get("property_type"), fields.get("price_metric"), fields.get("period"),
            vector
        )

        try:
            with psycopg.connect(DB_CONN_STRING) as conn:
                with conn.cursor() as cur:
                    # 테이블 생성 (없을 시)
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS user_query (
                            LOG_ID SERIAL PRIMARY KEY,
                            USER_ID VARCHAR(50), RAW_QUERY TEXT,
                            INTENT VARCHAR(50), LOCATION VARCHAR(100), COMPLEX_NAME VARCHAR(100),
                            PROPERTY_TYPE VARCHAR(50), PRICE_METRIC VARCHAR(50), PERIOD VARCHAR(50),
                            QUERY_VECTOR vector(768), CREATED_AT TIMESTAMP DEFAULT NOW()
                        );
                    """)
                    cur.execute(insert_sql, params)
                    conn.commit()
                    print("✅ [DB] 쿼리 로그 저장 완료.")
        except Exception as e:
            print(f"❌ [DB Error] 저장 실패: {e}")

if __name__ == "__main__":
    # 자체 테스트용 코드
    processor = StandaloneQueryProcessor()
    
    # 테스트용 파싱된 데이터
    test_payload = {
        "main_intent": "시세 조회",
        "location": "강남구",
        "complex_name": "은마아파트",
        "property_type": "아파트",
        "price_metric": "전세",
        "period": "최근 6개월"
    }
    
    # 이제 parsed_query 인자를 인식합니다.
    processor.process_and_save(parsed_query=test_payload, user_id="test_user")