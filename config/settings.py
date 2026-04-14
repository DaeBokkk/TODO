import os
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # ---------------------------------------------------------
    # 1. 모델 설정 (Llama 우선)
    # ---------------------------------------------------------
    MODEL_NAME: str = "llama-3-8b"
    LLAMA_MODEL_PATH: str = "./models/llama-3-8b.gguf"

    # API 키
    OPENAI_API_KEY: str | None = None
    GEMINI_API_KEY: str | None = None

    # 생성 파라미터
    TEMPERATURE: float = 0.1
    MAX_TOKENS: int = 512
    
    # 설계서 반영
    PROMPT_VERSION: str = "v1"

    # ---------------------------------------------------------
    # 2. 설정 로드 규칙
    # ---------------------------------------------------------
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

settings = Settings()