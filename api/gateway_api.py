from fastapi import APIRouter, HTTPException
from schemas.gateway import PreprocessRequest, PreprocessedResponse, AnswerResponse

# [모듈 로딩]
from modules.preprocessing.extractor import field_extractor 
from modules.preprocessing.guidance import guidance_module
from core.memory.history import history_manager
from core.retrieval.service import retrieval_service  # 검색 연결 서비스
from core.utils.prompt_builder import prompt_builder
from core.gateway.adapters import llama_engine
from config.settings import settings

router = APIRouter()

# ==============================================================================
# 1. 전처리 전용 API (리트리버 팀원에게 넘겨줄 데이터 생성용)
# ==============================================================================
@router.post("/preprocess", response_model=PreprocessedResponse)
async def preprocess_query(request: PreprocessRequest):
    """
    [1단계: 질의 전처리 API]
    사용자 질문을 받아 '기억(History)'을 참고하여 6대 핵심 필드를 파싱하고,
    필수 정보가 누락되었으면 리딩(Guidance) 질문을, 
    완벽하면 리트리버에게 넘길 데이터(JSON)를 반환합니다.
    """
    try:
        user_query = request.prompt.user
        session_id = request.prompt.session_id
        
        print(f"\n📩 [전처리] 사용자({session_id}): {user_query}")

        # 1. 사용자 질문을 기억 저장소에 기록
        history_manager.add_user_message(session_id, user_query)

        # 2. [추출] 이번 질문에서 새로운 정보만 뽑아냄 (현재 질문 집중)
        newly_extracted = field_extractor.extract(user_query, session_id=None) 
        print(f"🆕 [이번 턴 추출]: {newly_extracted}")

        # 3. [병합] 기존 기억(State) + 새로운 정보(New) = 최종 상태(6대 필드 누적)
        final_state = history_manager.update_state(session_id, newly_extracted)
        print(f"🧠 [최종 병합 상태]: {final_state}")

        # 4. [검사] 합쳐진 최종 상태를 기준으로 누락 확인
        missing = guidance_module.detect_missing(final_state)
        
        # [Case A] 여전히 필수 정보가 부족함 -> 되묻기 (Stop)
        if missing:
            guidance_msg = guidance_module.get_guidance_message(missing)
            
            # AI의 되묻는 질문도 기억해야 대화 흐름이 안 끊김
            history_manager.add_system_message(session_id, guidance_msg)
            
            print(f"⚠️ [리딩] {guidance_msg}")
            
            return PreprocessedResponse(
                status="needs_input",
                guidance_message=guidance_msg,
                final_query_frame=final_state,
                missing_fields=missing
            )

        # [Case B] 정보 완성 -> 성공 (Success)
        print("✅ [완료] 전처리 성공! 리트리버에게 넘길 데이터 완성")
        
        return PreprocessedResponse(
            status="success",
            guidance_message=None,
            final_query_frame=final_state, 
            missing_fields=[]
        )

    except Exception as e:
        print(f"❌ [Error] {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ==============================================================================
# 2. 통합 채팅 API (전처리 -> 검색 -> 생성 원스톱 서비스)
# ==============================================================================
@router.post("/chat", response_model=AnswerResponse)
async def chat_pipeline(request: PreprocessRequest):
    """
    [통합 파이프라인]
    1. 전처리 (파싱/리딩/기억) -> 정보 부족 시 되묻기 반환
    2. 검색 (멤버 B 서버 호출) -> 실패 시 에러 반환 (AI 호출 차단)
    3. 생성 (최종 답변)
    """
    try:
        user_query = request.prompt.user
        session_id = request.prompt.session_id
        print(f"\n🚀 [Chat] 통합 파이프라인 시작 (Session: {session_id})")

        # -------------------------------------------------------
        # 1단계: 전처리 (파싱 & 리딩 & 기억)
        # -------------------------------------------------------
        history_manager.add_user_message(session_id, user_query)
        
        extracted_fields = field_extractor.extract(user_query, session_id=None)
        final_state = history_manager.update_state(session_id, extracted_fields)
        
        missing = guidance_module.detect_missing(final_state)
        
        if missing:
            guidance_msg = guidance_module.get_guidance_message(missing)
            history_manager.add_system_message(session_id, guidance_msg)
            print(f"⚠️ [리딩] {guidance_msg}")
            
            return AnswerResponse(
                answer=guidance_msg,
                sources=[],
                confidence=1.0,
                model="system-guidance"
            )

        # -------------------------------------------------------
        # 2단계: 검색 (멤버 B 서버 호출)
        # -------------------------------------------------------
        print(f"✅ [전처리 완료] 검색 서버로 데이터 전송...")
        
        retrieved_context = retrieval_service.search(final_state, user_query)
        
        print(f"📚 [검색 완료] 문서 길이: {len(retrieved_context)}자")
        # print(f"👀 [Context 내용 미리보기]:\n{retrieved_context[:200]}...") 

        # 🚨 [추가된 안전장치] 검색이 실패했거나 내용이 너무 짧으면 AI 생성 차단!
        # 멤버 B 서버가 500 에러를 냈거나 데이터가 없을 때 환각 방지
        if "오류" in retrieved_context or "실패" in retrieved_context or len(retrieved_context) < 50:
            print("🛑 [중단] 검색 실패/오류로 인해 AI 생성을 차단합니다.")
            
            error_msg = "죄송합니다. 현재 부동산 데이터베이스 서버와 연결이 원활하지 않아 정보를 가져올 수 없습니다. 잠시 후 다시 시도해 주세요."
            
            return AnswerResponse(
                answer=error_msg,
                sources=["system_error"],
                confidence=0.0,
                model="system"
            )

        # -------------------------------------------------------
        # 3단계: 최종 답변 생성 (Llama)
        # -------------------------------------------------------
        final_prompt = prompt_builder.build(
            query=user_query,
            context=retrieved_context
        )
        
        print("🍳 [Llama] 답변 생성 중...")
        answer = llama_engine.generate(final_prompt)
        
        # 답변 정제 및 기억 저장
        clean_answer = answer.replace("### Answer:", "").replace("[답변]", "").strip()
        history_manager.add_system_message(session_id, clean_answer)
        
        print(f"🎉 [최종 완료] 답변 생성 끝")

        return AnswerResponse(
            answer=clean_answer,
            sources=["member_b_db"],
            confidence=0.95,
            model=settings.MODEL_NAME
        )

    except Exception as e:
        print(f"❌ [Pipeline Error] {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))