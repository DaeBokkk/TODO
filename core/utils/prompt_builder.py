import json
import os
from config.settings import settings

class PromptBuilder:
    def __init__(self):
        self.template_path = os.path.join(
            "config", 
            "prompt_templates", 
            "generation", 
            f"generation_prompt_{settings.PROMPT_VERSION}.json"
        )
        self.template = self._load_template()

    def _load_template(self) -> dict:
        if not os.path.exists(self.template_path):
            raise FileNotFoundError(f"❌ 프롬프트 템플릿을 찾을 수 없습니다: {self.template_path}")
        
        with open(self.template_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def build(self, query: str, context: str) -> str:
        system_text = self.template.get("system", "")
        context_text = self.template.get("context", "").replace("{context}", context)
        user_text = self.template.get("user", "").replace("{query}", query)
        instructions = "\n".join([f"- {inst}" for inst in self.template.get("instruction", [])])

        # 맨 마지막 assistant 태그 뒤에 "한국어 시작 멘트"를 미리 넣어둡니다.
        # 이렇게 하면 Llama는 이 뒷말을 이어서 한국어로 말할 수밖에 없습니다.
        final_prompt = f"""
<|begin_of_text|><|start_header_id|>system<|end_header_id|>

{system_text}

[지시사항]
{instructions}

[참고 문서]
{context_text}<|eot_id|><|start_header_id|>user<|end_header_id|>

{user_text}<|eot_id|><|start_header_id|>assistant<|end_header_id|>

[답변]
"""
        return final_prompt

prompt_builder = PromptBuilder()