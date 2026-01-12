import uvicorn
from fastapi import FastAPI
from config.settings import settings
from fastapi.middleware.cors import CORSMiddleware  # UI 연결 필수 모듈
from dotenv import load_dotenv  # .env 파일 로드

from api.gateway_api import router as processing_router
from api.generation_api import router as generation_router

# 0. 환경변수 로드 (앱 시작 전 확실하게 .env 내용을 읽어옵니다)
load_dotenv()

# 1. 앱 초기화
app = FastAPI(
    title="RAG Project API",
    version="1.0.0",
    description="LLM Dynamic Gateway API (Powered by Llama-3)"
)

# [중요] 2. CORS 미들웨어 설정
# 이 부분이 없으면 UI(프론트엔드)에서 백엔드로 요청을 보낼 때 'CORS Error'가 발생하여 막힙니다.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],      # 모든 도메인에서의 접속 허용 (보안상 개발 단계에서만 사용)
    allow_credentials=True,
    allow_methods=["*"],      # 모든 HTTP 메소드(GET, POST, OPTIONS 등) 허용
    allow_headers=["*"],      # 모든 헤더 허용
)

# 3. 라우터 연결 (API 주소 등록)
# 기존에 작성하신 대로 유지합니다.
app.include_router(processing_router, prefix="/v1", tags=["Preprocessing"])
app.include_router(generation_router, prefix="/v1", tags=["Generation"])

# 4. 서버 상태 확인 (Health Check)
@app.get("/")
def health_check():
    return {
        "status": "ok", 
        "model": settings.MODEL_NAME,
        "message": "Backend is running and ready for UI connection"
    }

if __name__ == "__main__":
    # host="0.0.0.0"은 외부(팀원)에서 내 IP로 접속할 수 있게 열어주는 설정입니다.
    print(f"🚀 Server starting on http://0.0.0.0:8000 (Model: {settings.MODEL_NAME})")
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)