from fastapi import APIRouter, HTTPException
from schemas.chat import ChatRequest, ChatResponse

# 모듈들을 모두 불러옴
from modules.preprocessing.extractor import field_extractor  # 질문 분석기
from core.retrieval.service import search_client           # 데이터 수집기
from core.gateway.adapters import gemini_engine            # 다정한 AI 엔진

router = APIRouter()

@router.post("/chat", response_model=ChatResponse)
async def chat_endpoint(request: ChatRequest):
    try:
        user_query = request.query
        
        # [1단계] 분석: 질문에서 키워드 추출 (extractor.py)
        extracted_payload = field_extractor.extract(user_query)
        
        # [2단계] 검색: aws 서버에서 데이터 확보 (service.py)
        # 추출된 JSON 데이터를 통째로 전달하여 검색함
        context_text = search_client.fetch_real_estate_data(user_query) 
        
        # [3단계] 생성: 다정한 말투로 답변 (adapters.py)
        prompt = f"""
옆에 계신 고객님께 조근조근 설명해드리는 전문가가 되어주세요.

[관련 실거래 자료]
{context_text}

[사용자 질문]
{user_query}
"""
        answer = gemini_engine.generate(prompt)
        
        return ChatResponse(
            answer=answer,
            model_used="gemini-3.1-pro-preview"
        )

    except Exception as e:
        print(f"❌ 배포 서버 에러: {str(e)}")
        raise HTTPException(status_code=500, detail="서버 내부 문제로 답변이 어렵네요.")