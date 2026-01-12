from langchain_community.llms import LlamaCpp
from config.settings import settings
import os

class LlamaAdapter:
    def __init__(self):
        print(f"🦙 Llama 모델 로딩 중... (경로: {settings.LLAMA_MODEL_PATH})")
        
        if not os.path.exists(settings.LLAMA_MODEL_PATH):
            raise FileNotFoundError(f"❌ 모델 파일이 없습니다! {settings.LLAMA_MODEL_PATH}")

        self.llm = LlamaCpp(
            model_path=settings.LLAMA_MODEL_PATH,
            temperature=0.1,          
            n_ctx=8192,
            verbose=False,
            # [수정] 한국어는 조사가 반복되므로 1.1이나 1.05가 적당합니다.
            repeat_penalty=1.2,
            top_p=0.1,
        )
        
        # [수정] 멈춤 신호도 심플하게
        self.default_stop = ["<|eot_id|>", "<|end_of_text|>", "User:", "질문:"]
        
        print("✅ Llama 모델 로딩 완료!")

    def generate(self, prompt_text: str, stop: list = None, max_tokens: int = 2048):
        try:
            final_stop = self.default_stop + (stop if stop else [])
            response = self.llm.invoke(
                prompt_text, 
                stop=final_stop,
                max_tokens=max_tokens
            )
            return response
        except Exception as e:
            return f"❌ 오류 발생: {str(e)}"

llama_engine = LlamaAdapter()