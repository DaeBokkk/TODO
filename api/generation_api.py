from fastapi import APIRouter, HTTPException
from schemas.gateway import GenerateFinalRequest, AnswerResponse
from core.gateway.adapters import llama_engine
from core.utils.prompt_builder import prompt_builder
from config.settings import settings

router = APIRouter()

@router.post("/generate", response_model=AnswerResponse)
async def generate_final_answer(request: GenerateFinalRequest):
    """
    [RAG Generation API]
    리트리버가 전달한 Context와 사용자 질문을 결합하여 최종 답변을 생성합니다.
    """
    try:
        print(f"\n📩 [Generation] 최종 생성 요청 수신")
        print(f"   - 질문: {request.query}")
        print(f"   - 컨텍스트 길이: {len(request.context)}자")

        # 1. 프롬프트 조립 (Prompt Builder 활용)
        # 설계서 프롬프트 구성
        final_prompt = prompt_builder.build(
            query=request.query,
            context=request.context
        )
        
        # 2. LLM 호출 (Generation)
        # 설계서 LLM 서비스 호출
        print("🍳 [Llama] 답변 생성 중...")
        raw_answer = llama_engine.generate(final_prompt)
        
        # 3. 후처리 (Post-Processing) - 간단한 정제
        # Llama가 가끔 "답변:" 이런 말을 붙이는데 제거
        clean_answer = raw_answer.replace("[답변]", "").replace("답변:", "").strip()

        print(f"✅ [완료] 생성된 답변: {clean_answer[:30]}...")

        # 4. 결과 반환 (Envelope)
        return AnswerResponse(
            answer=clean_answer,
            sources=[], # (고급 기능) 나중에 Llama가 출처를 뽑게 하면 채울 수 있음
            confidence=0.95, # 프로토타입이라 고정값 사용
            model=settings.MODEL_NAME
        )

    except Exception as e:
        print(f"❌ [Error] 생성 실패: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))