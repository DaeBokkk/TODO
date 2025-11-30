import json
import os
import re
from config.settings import settings
from core.gateway.adapters import llama_engine
from core.memory.history import history_manager

class FieldExtractor:
    def __init__(self):
        self.template_path = os.path.join(
            "config", "prompt_templates", "preprocessing", "parse_prompt_v1.json"
        )
        self.template = self._load_template()

    def _load_template(self) -> dict:
        if not os.path.exists(self.template_path):
            raise FileNotFoundError(f"❌ 파일을 찾을 수 없음: {self.template_path}")
        with open(self.template_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def extract(self, query: str, session_id: str = None) -> dict:
        system_text = self.template["system"]
        user_text = self.template["user"].replace("{query}", query)
        instructions = "\n".join([f"- {inst}" for inst in self.template["instruction"]])
        
        examples_text = ""
        if "examples" in self.template:
            examples_text = "\n[예시 데이터]\n"
            for ex in self.template["examples"]:
                examples_text += f"Q: {ex['query']}\nA: {json.dumps(ex['output'], ensure_ascii=False)}\n"

        # 프롬프트: 오직 현재 질문만 분석
        prompt = f"""
<|begin_of_text|><|start_header_id|>system<|end_header_id|>

{system_text}

[지시사항]
{instructions}

{examples_text}
<|eot_id|><|start_header_id|>user<|end_header_id|>

{user_text}<|eot_id|><|start_header_id|>assistant<|end_header_id|>
"""
        print(f"🔍 [Extractor] 6대 필드 추출 시도... 질문: {query}")
        raw_response = llama_engine.generate(prompt)
        
        # 4. [핵심 수정] 정규표현식으로 JSON만 정밀 타격
        try:
            # 1) 가장 먼저 나오는 { ... } 패턴을 찾음 (re.DOTALL은 줄바꿈 포함 검색)
            match = re.search(r'\{.*?\}', raw_response, re.DOTALL)
            
            if match:
                json_str = match.group() # 찾은 것만 가져옴
                return json.loads(json_str)
            else:
                # 2) 만약 { } 가 없으면 강제로라도 만들어 봄 (Fallback)
                print(f"⚠️ JSON 형식이 발견되지 않음. 원본: {raw_response}")
                return self._return_empty()
                
        except json.JSONDecodeError:
            print(f"⚠️ [Error] 파싱 실패. 원본: {raw_response}")
            return self._return_empty()

    def _return_empty(self):
        """실패 시 기본값 반환"""
        return {
            "main_intent": None, "location": None, "complex_name": None, 
            "property_type": None, "price_metric": None, "period": None
        }

field_extractor = FieldExtractor()