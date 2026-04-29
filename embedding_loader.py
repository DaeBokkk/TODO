import os
import gc
from langchain_huggingface import HuggingFaceEmbeddings
import dotenv

# ----------------------------------------------------
# 1. 단일 모델 슬롯 (Singleton Pattern)
# ----------------------------------------------------
# 메모리 자원이 극도로 제한된 환경(1GB RAM)에서 OOM(Out of Memory) 
# 강제 종료를 방지하기 위해 오직 하나의 임베딩 모델만 메모리에 유지합니다.
_current_model = None
_current_model_name = None

# [수정됨] 기본 모델을 "ko-sbert"에서 유료 모델인 "openai-v3"로 변경
def load_embedding_model(model_name: str = "openai-v3"):
    """
    요청된 임베딩 모델을 동적으로 로드(Lazy Loading)합니다.
    자원 보호를 위해 기존 모델을 메모리에서 완전히 삭제한 뒤 새 모델을 적재합니다.
    """
    global _current_model, _current_model_name

    # 1. Cache Hit: 이미 로드된 모델 재요청 시 중복 적재를 막고 즉시 반환
    if _current_model_name == model_name and _current_model is not None:
        return _current_model

    print(f"📥 [System] 자원 보호를 위해 기존 모델을 해제하고 '{model_name}' 로딩을 시작합니다...")

    # 2. 강제 메모리 회수: 새로운 400MB급 모델을 올리기 전 RAM 여유 공간을 즉시 확보
    if _current_model is not None:
        del _current_model
        _current_model = None
        gc.collect()
        print("♻️ [Memory] 가비지 컬렉션 완료") 

    try:
        if model_name == "ko-sbert":
            # GPU가 없는 저사양 환경에 맞춰 연산 장치를 CPU로 강제 할당
            _current_model = HuggingFaceEmbeddings(
                model_name="jhgan/ko-sbert-nli",
                model_kwargs={'device': 'cpu'},
                encode_kwargs={'normalize_embeddings': True}
            )
        elif model_name == "kcbert":
            _current_model = HuggingFaceEmbeddings(
                model_name="beomi/kcbert-base", 
                model_kwargs={'device': 'cpu'},
                encode_kwargs={'normalize_embeddings': True}
            )
        elif model_name == "openai-v3":
            # [수정됨] 데이터 적재 코드와 동일하게 API 키 직접 할당 및 모듈 적용
            from langchain_openai import OpenAIEmbeddings
            
            # 앞서 사용하신 API 키를 안전하게 적용
            dotenv.load_dotenv()
            OPENAI_API_KEY =  os.getenv('Emb_KEY')
            
            _current_model = OpenAIEmbeddings(
                model="text-embedding-3-small",
                openai_api_key=OPENAI_API_KEY
            )
        else:
            raise ValueError(f"지원하지 않는 모델: {model_name}")
            
        _current_model_name = model_name
        return _current_model

    except Exception as e:
        print(f"❌ '{model_name}' 로드 실패: {e}")
        
        # [수정됨] 장애 발생 시 Fallback(대체) 모델도 유료 모델로 설정
        if model_name != "openai-v3":
            print("🔄 기본 모델(openai-v3)로 Fallback을 시도합니다.")
            return load_embedding_model("openai-v3")
        else:
            raise RuntimeError("기본 임베딩 모델 로드에 완전히 실패했습니다.")