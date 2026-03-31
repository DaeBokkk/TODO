# Gemeni
import google.generativeai as genai
from config.settings import settings
from dotenv import load_dotenv
import os

# 환경변수 로드
load_dotenv(override=True)

class LLMAdapter:
    def __init__(self):
        
        # 1. API 키 확인
        api_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
        if not api_key:
            raise ValueError(" .env 파일에 API 키가 없습니다.")

        # 2. 구글 순정 라이브러리 설정
        genai.configure(api_key=api_key)

        # 3. 모델 선택
        self.model_name = "gemini-3.1-pro-preview"
        
        try:
            self.model = genai.GenerativeModel(self.model_name)
            print(f"✅ Gemini 모델 연결 완료! (Target: {self.model_name})")
        except Exception as e:
            print(f"❌ 모델 초기화 실패: {e}")

    def generate(self, prompt_text: str, stop: list = None, max_tokens: int = 4096):
        """
        순정 라이브러리를 사용해 답변을 생성합니다.
        """
        try:
            # 안전 설정 (불필요한 차단 방지)
            safety_settings = [
                {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
                {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
                {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
                {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
            ]

            # 생성 설정
            generation_config = genai.types.GenerationConfig(
                temperature=0.1,
                max_output_tokens=max_tokens,
                stop_sequences=stop if stop else []
            )

            # 생성 요청
            response = self.model.generate_content(
                prompt_text,
                generation_config=generation_config,
                safety_settings=safety_settings
            )

            # 결과 반환
            return response.text

        except Exception as e:
            # 429(한도 초과)나 404 등 에러 발생 시 로그 출력
            print(f"🧨 [Gemini Error] {str(e)}")
            return f"죄송합니다. AI 서버 오류가 발생했습니다. ({str(e)})"

# 외부에서 사용할 객체
gemini_engine = LLMAdapter()