from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any

# ==========================================
# 1. [전처리 단계] 사용자 질문 받아서 분석하기
# ==========================================

# 1-1. 전처리 요청 (Request) - 이름 변경: PreprocessRequest
class PromptParts(BaseModel):
    user: str = Field(..., description="사용자 질문")
    session_id: str = Field(..., description="사용자 식별 ID (예: user1)")
    context: Optional[str] = Field(default=None, description="테스트용 컨텍스트")

class PreprocessRequest(BaseModel):
    prompt: PromptParts

# 1-2. 전처리 응답 (Response) -> 리트리버에게 줄 데이터
# 설계서 PreprocessedQueryEnvelope 구조 반영
class PreprocessedResponse(BaseModel):
    status: str = Field(..., description="처리 상태 (success / needs_input)")
    guidance_message: Optional[str] = Field(default=None, description="정보 누락 시 되묻는 질문")
    final_query_frame: Optional[Dict[str, Any]] = Field(default=None, description="검색 팀에게 넘겨줄 핵심 파싱 데이터")
    missing_fields: List[str] = Field(default=[], description="누락된 필드 목록")


# ==========================================
# 2. [생성 단계] 검색된 문서 받아서 답변하기
# ==========================================

# 2-1. 최종 생성 요청 (Request) - 이름 변경: GenerateRequest -> GenerateFinalRequest
# 리트리버가 검색 끝내고 팀장님께 다시 보낼 데이터 구조
class GenerateFinalRequest(BaseModel):
    query: str = Field(..., description="사용자의 원래 질문")
    context: str = Field(..., description="리트리버가 찾아온 검색 결과(Context)")

# 2-2. 최종 생성 응답 (Response) -> 사용자에게 보여줄 최종 답변
# 설계서 AnswerResponse 구조 반영
class AnswerResponse(BaseModel):
    answer: str = Field(..., description="LLM이 생성한 최종 답변")
    sources: List[str] = Field(default=[], description="참고한 문서 출처")
    confidence: float = Field(default=1.0, description="답변 신뢰도")
    model: str = Field(..., description="사용된 모델명")