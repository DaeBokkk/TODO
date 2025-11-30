from fastapi import FastAPI
from config.settings import settings

from api.gateway_api import router as processing_router
from api.generation_api import router as generation_router

# 1. 앱 초기화
app = FastAPI(
    title="RAG Project API",
    version="1.0.0",
    description="LLM Dynamic Gateway API (Powered by Llama-3)"
)

# 2. 라우터 연결 (API 주소 등록)
# /v1/generate 주소로 요청이 오면 gateway_api가 처리하도록 설정
app.include_router(processing_router, prefix="/v1", tags=["Preprocessing"])
app.include_router(generation_router, prefix="/v1", tags=["Generation"])

# 3. 서버 실행 메시지
@app.get("/")
def health_check():
    return {"status": "ok", "model": settings.MODEL_NAME}

if __name__ == "__main__":
    import uvicorn
    # 0.0.0.0으로 열어야 팀원들도 접속 가능
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)