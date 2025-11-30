import time
import psycopg  # PostgreSQL 연결 (pip install psycopg psycopg-binary)
from typing import Dict, Any, List

# ------------------------------------------------------------------------------
# 1. 모듈 및 환경 설정
# ------------------------------------------------------------------------------

# 임베딩 모델 로더 (기존 모듈 재사용 또는 직접 로드)
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

# DB 연결 정보 (Ngrok 또는 로컬 설정)
DB_INFO = {
    "host": "0.tcp.jp.ngrok.io",
    "port": "17339",
    "user": "rag",
    "password": "rag",
    "dbname": "rag"
}
DB_CONN_STRING = f"host={DB_INFO['host']} port={DB_INFO['port']} user={DB_INFO['user']} password={DB_INFO['password']} dbname={DB_INFO['dbname']}"

# ------------------------------------------------------------------------------
# 2. 쿼리 처리 메인 클래스 (독립형)
# ------------------------------------------------------------------------------
class StandaloneQueryProcessor:
    def __init__(self):
        """
        서버 시작 시 모델을 메모리에 로드합니다.
        """
        print(" [Init] 독립형 쿼리 프로세서 초기화 중...")
        self.embedding_model = load_embedding_model()
        print(" [Init] 초기화 완료.")

    def _simple_extract(self, query: str) -> Dict[str, Any]:
        """
        [핵심 수정] 외부 FieldExtractor 없이 내부에서 간단히 파싱하는 로직입니다.
        실제 서버에서는 여기에 LLM 호출 로직이나 정규식 파서를 넣으면 됩니다.
        """
        print(f"   -> [Parser] 내부 파서 실행 중... (Query: {query})")
        
        # 예시: "강남구 은마아파트 전세" -> 간단한 키워드 매칭 (Rule-based)
        extracted = {
            "main_intent": "정보 검색",
            "location": None,
            "complex_name": None,
            "property_type": None,
            "price_metric": None,
            "period": None
        }
        
        # 간단한 규칙 예시 (실제로는 더 복잡한 로직 필요)
        if "강남" in query: extracted["location"] = "강남구"
        if "은마" in query: extracted["complex_name"] = "은마아파트"
        if "전세" in query: extracted["price_metric"] = "전세"
        if "아파트" in query: extracted["property_type"] = "아파트"
        
        return extracted

    def _convert_fields_to_text(self, fields: Dict[str, Any]) -> str:
        """
        파싱된 6개 필드를 임베딩하기 좋은 자연어 문장으로 변환합니다.
        """
        parts = []
        if fields.get("location"): parts.append(f"지역: {fields['location']}")
        if fields.get("complex_name"): parts.append(f"단지명: {fields['complex_name']}")
        if fields.get("property_type"): parts.append(f"유형: {fields['property_type']}")
        if fields.get("price_metric"): parts.append(f"가격기준: {fields['price_metric']}")
        
        # 변환된 텍스트가 없으면 기본값 반환
        return ", ".join(parts) if parts else "부동산 정보 검색"

    def process_and_save(self, user_query: str, user_id: str = "anonymous") -> Dict[str, Any]:
        """
        사용자 쿼리를 받아 내부 파싱 -> 임베딩 -> DB 저장을 수행합니다.
        """
        print(f"\n🚀 [Step 1] 사용자 쿼리 수신: '{user_query}'")
        
        # 1. 내부 파싱 로직 실행
        parsed_fields = self._simple_extract(user_query)
        print(f"   -> 파싱 결과: {parsed_fields}")
        
        # 2. 임베딩을 위한 텍스트 변환
        search_text = self._convert_fields_to_text(parsed_fields)
        
        # 3. 벡터 생성 (Embedding)
        try:
            query_vector = self.embedding_model.embed_query(search_text)
            print(f"   -> 벡터 생성 완료 (차원: {len(query_vector)})")
        except Exception as e:
            print(f" [Error] 임베딩 실패: {e}")
            return {}

        # 4. DB에 저장 (Insert)
        self._insert_query_log(user_id, user_query, parsed_fields, query_vector)
        
        return {
            "parsed_fields": parsed_fields,
            "vector": query_vector,
            "search_text": search_text
        }

    def _insert_query_log(self, user_id: str, query: str, fields: Dict, vector: List[float]):
        """
        파싱된 정보와 벡터를 'user_query' 테이블에 저장합니다.
        """
        # [수정] 테이블명을 "user_query" (소문자)로 변경
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
                    # 테이블 생성 (없을 시) - 테이블명 user_query로 수정
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
                    print(" [DB] 쿼리 로그 저장 완료.")
        except Exception as e:
            print(f" [DB Error] 저장 실패: {e}")

# ------------------------------------------------------------------------------
# 3. 실행 테스트
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    processor = StandaloneQueryProcessor()
    
    # 테스트 쿼리
    test_q = "강남구 은마아파트 전세 시세 알려줘"
    result = processor.process_and_save(test_q, user_id="tester_standalone")
    
    if result:
        print("\n--- [최종 처리 결과] ---")
        print(f"입력: {test_q}")
        print(f"파싱된 필드: {result['parsed_fields']}")
        print(f"변환된 검색 텍스트: '{result['search_text']}'")
        
        # [추가] 임베딩 결과값 출력 (앞 10개만)
        vector = result['vector']
        print(f"생성된 벡터 (총 {len(vector)}차원): {vector[:10]} ...")
        
        print("DB 저장 완료.")