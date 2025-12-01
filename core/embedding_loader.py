#embedding_loader.py

import json
from langchain_huggingface import HuggingFaceEmbeddings

# ------------------------------------------------------------------------------
# 1. 임베딩 모델 로딩 함수 (서버 실행 시 1회만 호출)
# ------------------------------------------------------------------------------
def load_embedding_model():
    print("📥 [System] 임베딩 모델(KO-SBERT) 로딩 중...")
    # Member A가 사용한 것과 동일한 모델명
    model_name = "jhgan/ko-sbert-nli"
    
    model = HuggingFaceEmbeddings(
        model_name=model_name,
        model_kwargs={'device': 'cpu'}, # GPU 서버라면 'cuda'로 변경 가능
        encode_kwargs={'normalize_embeddings': True}
    )
    print("✅ [System] 임베딩 모델 로딩 완료!")
    return model

# ------------------------------------------------------------------------------
# 2. 딕셔너리 -> 검색용 문자열 변환 함수
# ------------------------------------------------------------------------------
def convert_dict_to_search_text(intent_data: dict) -> str:
    """
    팀장님이 보내준 JSON 데이터(딕셔너리)를 검색하기 좋게 문자열로 변환합니다.
    """
    # ensure_ascii=False: 한글 깨짐 방지
    # separators: 공백 제거로 토큰 절약
    return json.dumps(intent_data, ensure_ascii=False, separators=(',', ':'))