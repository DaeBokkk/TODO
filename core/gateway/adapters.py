from langchain_community.llms import LlamaCpp
from langchain_core.callbacks import CallbackManager, StreamingStdOutCallbackHandler
from langchain_core.prompts import PromptTemplate
from config.settings import settings
import os

class LlamaAdapter:
    def __init__(self):
        print(f"🦙 Llama 모델 로딩 중... (경로: {settings.LLAMA_MODEL_PATH})")
        
        # 모델 파일이 진짜 있는지 확인
        if not os.path.exists(settings.LLAMA_MODEL_PATH):
            raise FileNotFoundError(f"❌ 모델 파일이 없습니다! {settings.LLAMA_MODEL_PATH} 위치에 파일을 넣어주세요.")

        # LlamaCpp 엔진 초기화 (내 컴퓨터 자원을 씀)
        self.llm = LlamaCpp(
            model_path=settings.LLAMA_MODEL_PATH,
            temperature=settings.TEMPERATURE,
            max_tokens=settings.MAX_TOKENS,
            n_ctx=2048,       # 문맥 길이 (Context Window)
            verbose=False,    # 잡다한 로그 끄기
            # n_gpu_layers=-1 # (중요) GPU가 있으면 주석 해제하세요! 속도가 빨라집니다.
        )
        print("✅ Llama 모델 로딩 완료!")

    def generate(self, prompt_text: str):
        """
        질문을 받아서 답변을 생성하는 함수
        """
        try:
            # Llama에게 질문 던지기
            response = self.llm.invoke(prompt_text)
            return response
        except Exception as e:
            return f"❌ 오류 발생: {str(e)}"

# 나중에 다른 파일에서 쉽게 쓰려고 미리 만들어둠
llama_engine = LlamaAdapter()