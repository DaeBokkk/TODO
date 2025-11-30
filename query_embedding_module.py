import json
from typing import List, Dict, Any

# 기존에 작성된 모듈들을 가져옵니다.
try:
    from extractor import FieldExtractor  # 사용자 쿼리 추출기 (FieldExtractor)
    from data_embedding_module import load_embedding_model # 임베딩 모델 로더
except ImportError:
    pass # 테스트 환경에서 파일이 없을 경우를 대비한 예외 처리

# ------------------------------------------------------------------------------
# 1. 딕셔너리 -> 검색 텍스트 변환 함수 (Preprocessing)
# ------------------------------------------------------------------------------
def convert_intent_to_text(intent_data: Dict[str, Any]) -> str:
    """
    FieldExtractor가 반환한 딕셔너리(키-값)를 임베딩 모델이 이해하기 좋은
    자연어 형태의 문자열로 변환합니다.
    
    이 과정이 필요한 이유:
    - 임베딩 모델(KO-SBERT)은 JSON 객체보다 '문장' 형태의 텍스트에서 의미를 더 잘 포착합니다.
    - 단순히 JSON을 문자열로 바꾸는 것보다, 의미 있는 키워드만 나열하는 것이 검색 정확도(Score)를 높입니다.

    Args:
        intent_data (Dict): 추출된 필드 데이터 
                            (예: {'location': '강남', 'property_type': '아파트', ...})

    Returns:
        str: 임베딩용 검색 텍스트 (예: "지역: 강남, 유형: 아파트")
    """
    search_keywords = []
    
    # _return_empty에 정의된 키들을 순회하며 유효한 값만 추출합니다.
    
    # 1. 지역 정보 (가장 중요)
    if intent_data.get("location"):
        search_keywords.append(f"지역: {intent_data['location']}")
        
    # 2. 단지명 (특정 아파트 검색 시)
    if intent_data.get("complex_name"):
        search_keywords.append(f"단지명: {intent_data['complex_name']}")
        
    # 3. 매물 유형 (아파트, 오피스텔 등)
    if intent_data.get("property_type"):
        search_keywords.append(f"유형: {intent_data['property_type']}")
        
    # 4. 가격 기준 (매매, 전세 등)
    if intent_data.get("price_metric"):
        search_keywords.append(f"거래방식: {intent_data['price_metric']}")
        
    # 5. 기간 정보
    if intent_data.get("period"):
        search_keywords.append(f"기간: {intent_data['period']}")
        
    # 6. 주 의도 (매수, 시세조회 등) - 검색 텍스트에는 포함하지 않거나 가중치를 낮출 수 있음
    # 여기서는 포함하여 문맥을 보강합니다.
    if intent_data.get("main_intent"):
        search_keywords.append(f"의도: {intent_data['main_intent']}")

    # 추출된 키워드가 하나도 없다면 빈 문자열 반환 (또는 원본 쿼리 사용 유도)
    if not search_keywords:
        return ""
        
    # "키: 값, 키: 값" 형태로 결합
    return ", ".join(search_keywords)

# ------------------------------------------------------------------------------
# 2. 구조화된 데이터 임베딩 함수 (Main Logic)
# ------------------------------------------------------------------------------
def embed_structured_query(
    user_query: str, 
    extractor: 'FieldExtractor', 
    embedding_model
) -> Dict[str, Any]:
    """
    사용자 쿼리를 받아 정보를 추출하고, 이를 벡터화하여 반환하는 메인 함수입니다.

    Args:
        user_query (str): 사용자의 자연어 질문 (예: "강남 아파트 시세 알려줘")
        extractor (FieldExtractor): 정보를 추출할 객체
        embedding_model: 로드된 LangChain Embeddings 객체

    Returns:
        Dict: {
            "original_query": 원본 질문,
            "extracted_data": 추출된 딕셔너리 (키-값),
            "search_text": 벡터화에 사용된 변환 텍스트,
            "vector": 생성된 임베딩 벡터 (List[float])
        }
    """
    print(f"\n[Process] 1. 사용자 의도 추출 시작... (질문: {user_query})")
    
    # 1. LLM을 사용하여 필드 추출 (extractor.py 활용)
    # 결과 예: {'location': '강남', 'property_type': '아파트', ...}
    extracted_data = extractor.extract(user_query)
    print(f" -> 추출 결과: {extracted_data}")

    # 2. 딕셔너리를 임베딩용 텍스트로 변환
    # 결과 예: "지역: 강남, 유형: 아파트, 의도: 시세조회"
    search_text = convert_intent_to_text(extracted_data)
    
    # 만약 추출된 정보가 너무 적으면(빈 값이면), 원본 쿼리를 그대로 사용합니다. (Fallback)
    if not search_text.strip():
        print(" -> ⚠️ 추출된 정보가 부족하여 원본 질문을 대신 사용합니다.")
        search_text = user_query
    else:
        print(f" -> 변환된 검색 텍스트: '{search_text}'")

    # 3. 텍스트 임베딩 (벡터화)
    # data_embedding_module.py의 모델을 사용
    print(f"[Process] 2. 벡터 변환 중...")
    vector = embedding_model.embed_query(search_text)
    
    print(f" -> [Success] 벡터 생성 완료 (차원: {len(vector)})")

    # 4. 최종 결과 반환
    return {
        "original_query": user_query,
        "extracted_data": extracted_data, # 이 데이터는 DB 필터링(Filter)에 사용됨
        "search_text": search_text,       # 실제 벡터화된 내용
        "vector": vector                  # DB 검색(Search)에 사용될 벡터
    }

# ------------------------------------------------------------------------------
# 테스트 실행 (Test Block)
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    # 1. 모듈 초기화 (실제 환경에서는 주입받아서 사용)
    # 주의: extractor.py와 data_embedding_module.py가 작동 가능한 상태여야 합니다.
    
    try:
        # 추출기 및 임베딩 모델 인스턴스 생성
        my_extractor = FieldExtractor()
        my_embedding_model = load_embedding_model()
        
        # 테스트 질문
        test_query = "서울 강남구 아파트 최근 1년 전세 시세 어때?"
        
        # 함수 실행
        result = embed_structured_query(test_query, my_extractor, my_embedding_model)
        
        # 결과 출력
        print("\n--------------------------------------------------")
        print("[최종 반환 데이터 구조]")
        print(f"1. 추출 데이터(Filter용): {result['extracted_data']}")
        print(f"2. 임베딩 텍스트(Search용): {result['search_text']}")
        print(f"3. 생성된 벡터(일부): {result['vector'][:5]} ...")
        print("--------------------------------------------------")
        
    except Exception as e:
        print(f"\n[Test Error] 모듈 실행 중 오류 발생: {e}")
        print("팁: extractor.py의 설정 파일 경로와 라이브러리 설치를 확인하세요.")