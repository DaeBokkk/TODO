import json
import os
from config.settings import settings

class PromptBuilder:
    def __init__(self):
        self.template_path = os.path.join(
            "config", "prompt_templates", "generation", 
            f"generation_prompt_{settings.PROMPT_VERSION}.json"
        )
        self.template = self._load_template()

    def _load_template(self) -> dict:
        if not os.path.exists(self.template_path):
            raise FileNotFoundError(f"❌ 프롬프트 템플릿을 찾을 수 없습니다: {self.template_path}")
        with open(self.template_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def build(self, query: str, context: str) -> str:
        system_text = "당신은 부동산 실거래가 정보를 요약해주는 AI 비서입니다."
        
        instructions_list = self.template.get("instruction", [])
        instructions = "\n".join([f"- {inst}" for inst in instructions_list])

        # [최종 필살기]
        # 1. System: 역할만 간단히
        # 2. User: 문맥과 질문, 그리고 '답변 형식'을 지정
        # 3. Assistant: [답변]이라는 헤더를 미리 박아서 "이제 답변 쓸 차례야"라고 강제 인식시킴
        
        final_prompt = f"""<|start_header_id|>system<|end_header_id|>

{system_text}<|eot_id|><|start_header_id|>user<|end_header_id|>

[검색된 부동산 데이터]
{context}

[사용자 질문]
{query}

[지시사항]
위 [검색된 부동산 데이터]를 바탕으로 질문에 대해 한국어로 답변하세요.
데이터에 있는 내용만 요약해서 설명하세요.<|eot_id|><|start_header_id|>assistant<|end_header_id|>

[답변]
네, 검색된 정보에 따르면"""
        
        # ↑ 핵심: "네, 검색된 정보에 따르면" 이라고 우리가 먼저 써줌.
        # 이제 모델은 이 뒤를 이어서 "평택 효성해링턴..." 하고 팩트를 말할 수밖에 없음.

        return final_prompt # strip 절대 금지 (뒤에 공백 유지)

prompt_builder = PromptBuilder()