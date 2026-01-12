from fastapi import APIRouter, HTTPException
from schemas.chat import ChatRequest, ChatResponse

# [중요] 실제 구현한 LLM 호출 엔진을 가져옴.
from core.gateway.adapters import llama_engine

router = APIRouter()

@router.post("/chat", response_model=ChatResponse)
async def chat_endpoint(request: ChatRequest):
    # 1. 모델 선택 로직 (나중에 GPT/Gemini 분기 처리 가능)
    if request.model_name == "llama-3-8b":
        # Llama 3용 프롬프트 포맷 적용 (이게 없으면 횡설수설할 수 있음)
        formatted_prompt = (
            f"<|begin_of_text|><|start_header_id|>user<|end_header_id|>\n\n"
            f"{request.query}<|eot_id|>"
            f"<|start_header_id|>assistant<|end_header_id|>\n\n"
        )
        
        # 2. 생성 (LlamaAdapter의 generate 메서드 호출)
        # invoke는 동기 함수이므로, CPU 부하가 큼. 
        # 간단한 구현을 위해 여기서는 직접 호출하지만 운영 환경에서는 run_in_threadpool 등을 고려해야 함.
        answer = llama_engine.generate(formatted_prompt)
        
        return ChatResponse(
            answer=answer,
            model_used="llama-3-8b-local"
        )

    else:
        # GPT나 다른 모델 로직 (추후 구현)
        return ChatResponse(
            answer="아직 지원되지 않는 모델입니다. model_name을 'llama-3-8b'로 설정해주세요.",
            model_used=request.model_name
        )