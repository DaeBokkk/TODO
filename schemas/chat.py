from pydantic import BaseModel, Field
from typing import Optional

# UI -> Server (요청 데이터)
class ChatRequest(BaseModel):
    query: str = Field(..., description="사용자 질문")
    model_name: Optional[str] = Field("gpt-4o", description="사용할 모델 (gpt-4o, gemini-1.5-pro, llama-3-8b)")

# Server -> UI (응답 데이터)
class ChatResponse(BaseModel):
    answer: str
    model_used: str