import json
import os
import re
import ast  # [핵심] 파이썬 자료형 문법을 해석하는 강력한 도구
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

        # [전략] 프롬프트 끝에 '{'를 미리 적어둬서 JSON 시작을 강제함
        prompt = f"""
<|begin_of_text|><|start_header_id|>system<|end_header_id|>

{system_text}

[지시사항]
{instructions}

{examples_text}
<|eot_id|><|start_header_id|>user<|end_header_id|>

{user_text}<|eot_id|><|start_header_id|>assistant<|end_header_id|>

{{
"""
        print(f"🔍 [Extractor] 파싱 시도: {query}")
        
        # 1. 생성 (닫는 괄호 '}'가 나오면 멈추게 함 -> 속도 향상 & 군더더기 방지)
        raw_response = llama_engine.generate(prompt, stop=["}"], max_tokens=256)
        
        # 2. JSON 복원 ('{' 와 '}'를 앞뒤로 붙여줌)
        full_json_str = "{" + raw_response.strip()
        if not full_json_str.endswith("}"):
            full_json_str += "}"

        # 3. 강력한 파싱 시도
        final_result = self._return_empty()
        parsed_data = self._robust_json_parse(full_json_str)
        
        if parsed_data:
            final_result.update(parsed_data)
        else:
            print(f"⚠️ [완전 실패] JSON 파싱 불가. 원본: {full_json_str}")
            # 실패하면 None이 유지되므로 리딩 모듈이 작동함

        return final_result

    def _robust_json_parse(self, text: str) -> dict:
        """
        [핵심] 어떤 형태의 JSON(유사 JSON)이 와도 딕셔너리로 변환해내는 함수
        """
        # 1차 시도: 정석 JSON 파싱
        try:
            return json.loads(text)
        except:
            pass
        
        # 2차 시도: 파이썬 리터럴 파싱 (작은따옴표, None 등을 처리해줌)
        try:
            # Llama가 null을 None으로, true를 True로 썼을 수도 있음
            # 텍스트 보정
            text_fixed = text.replace("null", "None").replace("true", "True").replace("false", "False")
            return ast.literal_eval(text_fixed)
        except:
            pass

        # 3차 시도: 정규식으로 강제 추출 (최후의 수단)
        return self._regex_parse(text)

    def _regex_parse(self, text: str) -> dict:
        result = {}
        keys = ["main_intent", "location", "complex_name", "property_type", "price_metric", "period"]
        for key in keys:
            # 키: 값 패턴 찾기 (따옴표 유무 상관없이)
            pattern = fr'[\'"]?{key}[\'"]?\s*:\s*[\'"]?([^,\}}]+)[\'"]?'
            match = re.search(pattern, text)
            if match:
                val = match.group(1).strip().strip('"').strip("'")
                if val.lower() not in ['null', 'none']:
                    result[key] = val
        return result

    def _return_empty(self):
        return {
            "main_intent": None, "location": None, "complex_name": None, 
            "property_type": None, "price_metric": None, "period": None
        }

field_extractor = FieldExtractor()