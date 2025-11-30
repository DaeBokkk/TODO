from fastapi import APIRouter, HTTPException
from schemas.gateway import PreprocessRequest, PreprocessedResponse

# [모듈 로딩]
from modules.preprocessing.extractor import field_extractor 
from modules.preprocessing.guidance import guidance_module
from core.memory.history import history_manager # 업데이트된 매니저

router = APIRouter()

@router.post("/preprocess", response_model=PreprocessedResponse)
async def preprocess_query(request: PreprocessRequest):
    try:
        user_query = request.prompt.user
        session_id = request.prompt.session_id
        
        print(f"\n📩 [전처리] 사용자({session_id}): {user_query}")

        # 1. 사용자 질문 기록
        history_manager.add_user_message(session_id, user_query)

        # 2. [추출] 이번 질문에서 새로운 정보만 뽑아냄 (기존 기억 안 섞음)
        # LLM에게 굳이 옛날 얘기 안 해줘도 됨 (파이썬이 합칠 거니까)
        newly_extracted = field_extractor.extract(user_query, session_id=None) 
        print(f"🆕 [이번 턴 추출]: {newly_extracted}")

        # 3. [병합] 기존 기억(State) + 새로운 정보(New) = 최종 상태
        final_state = history_manager.update_state(session_id, newly_extracted)
        print(f"🧠 [최종 병합 상태]: {final_state}")

        # 4. [검사] 합쳐진 최종 상태를 기준으로 누락 확인
        missing = guidance_module.detect_missing(final_state)
        
        # [Case A] 여전히 부족함 -> 되묻기
        if missing:
            guidance_msg = guidance_module.get_guidance_message(missing)
            history_manager.add_system_message(session_id, guidance_msg)
            
            print(f"⚠️ [리딩] {guidance_msg}")
            
            return PreprocessedResponse(
                status="needs_input",
                guidance_message=guidance_msg,
                final_query_frame=final_state, # 현재까지 모은 거라도 보여줌
                missing_fields=missing
            )

        # [Case B] 정보 완성 -> 성공
        print("✅ [완료] 모든 필수 정보 수집 완료!")
        
        return PreprocessedResponse(
            status="success",
            guidance_message=None,
            final_query_frame=final_state, # 완성된 데이터 전달
            missing_fields=[]
        )

    except Exception as e:
        print(f"❌ [Error] {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))