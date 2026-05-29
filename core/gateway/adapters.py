# import os
# from dotenv import load_dotenv
# from groq import Groq

# # 환경 변수를 로드한다.
# load_dotenv(override=True)

# class LlamaAdapter:
#     def __init__(self):
#         # 환경 변수에서 Groq API 키를 확인한다.
#         api_key = os.getenv("GROQ_API_KEY")
#         if not api_key:
#             raise ValueError("❌ GROQ_API_KEY가 .env 파일에 설정되지 않음!")

#         print("🚀 초고속 Groq API 기반 Llama 모델 로딩 중...")
        
#         # Groq 클라이언트를 초기화한다.
#         self.client = Groq(api_key=api_key)
        
#         # [수정 1] 단종된 모델 대신 최신 3.1 8B 모델을 지정한다.
#         self.model_name = "llama-3.1-8b-instant" 
        
#         # 기본 멈춤 신호를 3개로 줄여서 설정한다.
#         self.default_stop = ["<|eot_id|>", "User:", "질문:"]
        
#         print(f"✅ Groq Llama 모델 연결 완료! (Target: {self.model_name})")

#     def generate(self, prompt_text: str, stop: list = None, max_tokens: int = 1024) -> str:
#         try:
#             # 기본 멈춤 신호와 전달받은 멈춤 신호를 병합한다.
#             final_stop = self.default_stop + (stop if stop else [])
            
#             # [수정 2] Groq API의 4개 제한을 넘지 않도록 리스트를 자른다.
#             final_stop = final_stop[:4] 
            
#             # API 호출을 통해 응답을 생성한다.
#             response = self.client.chat.completions.create(
#                 model=self.model_name,
#                 messages=[
#                     {"role": "user", "content": prompt_text}
#                 ],
#                 temperature=0.1, 
#                 max_tokens=max_tokens,
#                 stop=final_stop
#             )
            
#             # 생성된 텍스트 결과만 추출하여 반환한다.
#             return response.choices[0].message.content
            
#         except Exception as e:
#             print(f"🧨 [LLaMA Error] {str(e)}")
#             return f"❌ 오류 발생: {str(e)}"

# llama_engine = LlamaAdapter()

# //////////////////////////////////////////////////////////////////////////////////////////////////////////////

# # Gemeni
# import google.generativeai as genai
# from config.settings import settings
# from dotenv import load_dotenv
# import os

# # 환경변수 로드
# load_dotenv(override=True)

# class LLMAdapter:
#     def __init__(self):
        
#         # 1. API 키 확인
#         api_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
#         if not api_key:
#             raise ValueError(" .env 파일에 API 키가 없습니다.")

#         # 2. 구글 순정 라이브러리 설정
#         genai.configure(api_key=api_key)

#         # 3. 모델 선택
#         self.model_name = "gemini-3.1-pro-preview"
        
#         try:
#             self.model = genai.GenerativeModel(self.model_name)
#             print(f"✅ Gemini 모델 연결 완료! (Target: {self.model_name})")
#         except Exception as e:
#             print(f"❌ 모델 초기화 실패: {e}")

#     def generate(self, prompt_text: str, stop: list = None, max_tokens: int = 4096):
#         """
#         순정 라이브러리를 사용해 답변을 생성합니다.
#         """
#         try:
#             # 안전 설정 (불필요한 차단 방지)
#             safety_settings = [
#                 {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
#                 {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
#                 {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
#                 {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
#             ]

#             # 생성 설정
#             generation_config = genai.types.GenerationConfig(
#                 temperature=0.1,
#                 max_output_tokens=max_tokens,
#                 stop_sequences=stop if stop else []
#             )

#             # 생성 요청
#             response = self.model.generate_content(
#                 prompt_text,
#                 generation_config=generation_config,
#                 safety_settings=safety_settings
#             )

#             # 결과 반환
#             return response.text

#         except Exception as e:
#             # 429(한도 초과)나 404 등 에러 발생 시 로그 출력
#             print(f"🧨 [Gemini Error] {str(e)}")
#             return f"죄송합니다. AI 서버 오류가 발생했습니다. ({str(e)})"

# # 외부에서 사용할 객체
# gemini_engine = LLMAdapter()

# ///////////////////////////////////////////////////////////////////////////////////////////////////////////////

#gpt
import os
from dotenv import load_dotenv

# 환경변수 로드
load_dotenv(override=True)

# ==========================================
# 1. GPT (OpenAI) 엔진 (신규 추가)
# ==========================================
from openai import OpenAI

class GPTAdapter:
    def __init__(self):
        self.api_key = os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError("❌ .env 파일에 OPENAI_API_KEY가 없습니다.")
        
        self.client = OpenAI(api_key=self.api_key)
        # 테스트 목적에 맞춰 gpt-4o, gpt-4-turbo, gpt-3.5-turbo 중 선택한다.
        self.model_name = "gpt-4o" 
        print(f"✅ GPT 모델 연결 완료! (Target: {self.model_name})")

    def generate(self, prompt_text: str, stop: list = None, max_tokens: int = 4096) -> str:
        """OpenAI API를 사용하여 답변을 생성한다."""
        try:
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {"role": "user", "content": prompt_text}
                ],
                temperature=0.1,  # 사실 기반 RAG이므로 환각 방지를 위해 온도를 낮춘다.
                max_tokens=max_tokens,
                stop=stop
            )
            return response.choices[0].message.content
        
        except Exception as e:
            print(f"🧨 [GPT Error] {str(e)}")
            return f"❌ GPT 서버 오류가 발생했습니다. ({str(e)})"

# 외부에서 사용할 GPT 객체 생성
gpt_engine = GPTAdapter()