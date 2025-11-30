import sys
import os

# 모듈 경로 설정 (필요 시)
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 작성해주신 임베딩 처리 모듈 가져오기
try:
    from query_embedding_module import StandaloneQueryProcessor
except ImportError:
    print("[Error] 'query_embedding_module.py' 파일을 찾을 수 없습니다.")
    sys.exit(1)

def main():
    """
    서버에서 파싱된 사용자 쿼리가 들어왔을 때, 
    임베딩하고 DB에 저장하는 메인 로직입니다.
    """
    print("🚀 [System] RAG 검색 서버 시스템 시작...\n")

    # 1. 프로세서 초기화 (서버 시작 시 1회만 수행하면 효율적)
    # 모델 로딩 시간이 포함되므로 서버 시작 시 미리 해두는 것이 좋습니다.
    processor = StandaloneQueryProcessor()
    print("\n" + "="*50)

    # ---------------------------------------------------------
    # [시나리오] 서버가 API 요청으로 받은 파싱된 쿼리 데이터 (가정)
    # query 변수에 이미 의도, 위치 등이 파싱되어 있다고 가정합니다.
    # ---------------------------------------------------------
    query_payload = {
        "main_intent": "실거래가 조회",     # 질문 의도
        "location": "수원시 영통구 원천동", # 지역 정보
        "complex_name": "광교중흥S클래스",  # 아파트/단지명
        "property_type": "아파트",          # 매물 유형
        "price_metric": "매매",             # 가격 기준 (매매/전세/월세)
        "period": "2024년"                 # 기간 정보
    }
    
    user_id = "user_test_01" # 요청한 사용자 ID

    print(f"📥 [Input] 사용자 쿼리 수신 (Parsed JSON):")
    print(f"   User ID: {user_id}")
    print(f"   Query Data: {query_payload}")
    print("-" * 50)

    # 2. 임베딩 및 DB 저장 실행
    # process_and_save 함수가 내부적으로 '자연어 변환 -> 임베딩 -> DB 저장'을 모두 수행합니다.
    try:
        result = processor.process_and_save(parsed_query=query_payload, user_id=user_id)
        
        # 3. 결과 확인 및 로그 출력
        if result:
            print("\n [Success] 쿼리 처리 및 DB 저장 완료!")
            print(f"   1. 변환된 검색 텍스트: '{result['search_text']}'")
            print(f"   2. 생성된 임베딩 벡터 차원: {len(result['vector'])} 차원")
            print(f"   3. 벡터 값 일부 확인: {result['vector'][:5]} ...")
            
            # 실제 검색(Retriever) 단계로 넘어간다면 여기서 result['vector']를 사용합니다.
            # search_results = vector_db.similarity_search(result['vector']) 
            
        else:
            print("\n [Fail] 쿼리 처리 중 오류가 발생했습니다.")
            
    except Exception as e:
        print(f"\n [Error] 실행 중 예외 발생: {e}")

if __name__ == "__main__":
    main()

### **사용 방법**

# 1.  위 코드를 **`main.py`** 파일로 저장합니다.
# 2.  같은 폴더에 **`query_embedding_module.py`**와 **`data_embedding_module.py`**가 있는지 확인합니다.
# 3.  터미널에서 실행합니다:
#     ```bash
#     python main.py