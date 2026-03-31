# import json
# import os
# import re
# import ast  # [핵심] 파이썬 자료형 문법을 해석하는 강력한 도구
# from config.settings import settings
# from core.gateway.adapters import llama_engine
# from core.memory.history import history_manager
# from datetime import datetime, timedelta

# class FieldExtractor:
#     def __init__(self):
#         self.template_path = os.path.join(
#             "config", "prompt_templates", "preprocessing", "parse_prompt_v1.json"
#         )
#         self.template = self._load_template()

#     def _load_template(self) -> dict:
#         if not os.path.exists(self.template_path):
#             raise FileNotFoundError(f"❌ 파일을 찾을 수 없음: {self.template_path}")
#         with open(self.template_path, "r", encoding="utf-8") as f:
#             return json.load(f)

#     # 🚨 [복구된 핵심 부분] extract 함수 선언 및 실행 흐름 재정렬
#     def extract(self, query: str, session_id: str = None) -> dict:
#         system_text = self.template["system"]
#         user_text = self.template["user"].replace("{query}", query)
#         instructions = "\n".join([f"- {inst}" for inst in self.template["instruction"]])
        
#         examples_text = ""
#         if "examples" in self.template:
#             examples_text = "\n[예시 데이터]\n"
#             for ex in self.template["examples"]:
#                 examples_text += f"Q: {ex['query']}\nA: {json.dumps(ex['output'], ensure_ascii=False)}\n"

#         # [전략] 프롬프트 끝에 '{'를 미리 적어둬서 JSON 시작을 강제한다.
#         prompt = f"""
# <|begin_of_text|><|start_header_id|>system<|end_header_id|>

# {system_text}

# [지시사항]
# {instructions}

# {examples_text}
# <|eot_id|><|start_header_id|>user<|end_header_id|>

# {user_text}<|eot_id|><|start_header_id|>assistant<|end_header_id|>

# {{
# """
#         print(f"🔍 [Extractor] 파싱 시도: {query}")
        
#         # 1. 생성 (닫는 괄호 '}'가 나오면 멈추게 한다 -> 속도 향상 & 군더더기 방지)
#         raw_response = llama_engine.generate(prompt, stop=["}"], max_tokens=256)
        
#         # 2. JSON 복원 ('{' 와 '}'를 앞뒤로 붙여준다.)
#         full_json_str = "{" + raw_response.strip()
#         if not full_json_str.endswith("}"):
#             full_json_str += "}"

#         # 3. 강력한 파싱 시도
#         final_result = self._return_empty()
#         parsed_data = self._robust_json_parse(full_json_str)
        
#         if parsed_data:
#             final_result.update(parsed_data)
#         else:
#             print(f"⚠️ [완전 실패] JSON 파싱 불가. 원본: {full_json_str}")
#             # 실패하면 None이 유지되므로 리딩 모듈이 작동한다.

#         # =========================================================
#         # 🚑 [추가된 파이썬 강제 보정 로직]
#         # =========================================================
#         # 1. 필수 키 구조 보장
#         for key in ["location", "complex_name", "property_type", "price_metric", "start_date", "end_date", "main_intent"]:
#             if key not in final_result: final_result[key] = None

#         # 2. 날짜(기간) 강제 주입 (디폴트 90일 세팅)
#         today = datetime.today()
#         if not final_result.get("end_date"):
#             final_result["end_date"] = today.strftime('%Y-%m-%d')
#         if not final_result.get("start_date"):
#             past_date = today - timedelta(days=90)
#             final_result["start_date"] = past_date.strftime('%Y-%m-%d')

#         # 3. 가격, 유형, 지역, 의도 강제 주입
#         if not final_result.get("price_metric"):
#             if "전세" in query: final_result["price_metric"] = "전세"
#             elif "월세" in query: final_result["price_metric"] = "월세"
#             else: final_result["price_metric"] = "매매" 

#         if not final_result.get("property_type"):
#             if "오피" in query: final_result["property_type"] = "오피스텔"
#             elif "빌라" in query: final_result["property_type"] = "빌라"
#             else: final_result["property_type"] = "아파트" 

#         if not final_result.get("location"):
#             loc_pattern = r"([가-힣]+(?:시|구|동))"
#             match = re.search(loc_pattern, query)
#             if match: final_result["location"] = match.group(1)

#         if not final_result.get("main_intent"):
#             if "전망" in query or "예측" in query or "어때" in query:
#                 final_result["main_intent"] = "전망"
#             else:
#                 final_result["main_intent"] = "시세"

#         print(f"✅ [최종 추출 결과] {final_result}")
#         return final_result

#     def _robust_json_parse(self, text: str) -> dict:
#         """
#         [핵심] 어떤 형태의 JSON(유사 JSON)이 와도 딕셔너리로 변환해내는 함수
#         """
#         # 1차 시도: 정석 JSON 파싱
#         try:
#             return json.loads(text)
#         except:
#             pass
        
#         # 2차 시도: 파이썬 리터럴 파싱 (작은따옴표, None 등을 처리해 준다.)
#         try:
#             # Llama가 null을 None으로, true를 True로 썼을 수도 있으므로 텍스트를 보정한다.
#             text_fixed = text.replace("null", "None").replace("true", "True").replace("false", "False")
#             return ast.literal_eval(text_fixed)
#         except:
#             pass

#         # 3차 시도: 정규식으로 강제 추출 (최후의 수단)
#         return self._regex_parse(text)

#     def _regex_parse(self, text: str) -> dict:
#         result = {}
#         # 🚨 [수정] period 대신 start_date, end_date로 변경
#         keys = ["main_intent", "location", "complex_name", "property_type", "price_metric", "start_date", "end_date"]
#         for key in keys:
#             pattern = fr'[\'"]?{key}[\'"]?\s*:\s*[\'"]?([^,\}}]+)[\'"]?'
#             match = re.search(pattern, text)
#             if match:
#                 val = match.group(1).strip().strip('"').strip("'")
#                 if val.lower() not in ['null', 'none']:
#                     result[key] = val
#         return result

#     def _return_empty(self):
#         # 🚨 [수정] period 대신 start_date, end_date로 변경
#         return {
#             "main_intent": None, "location": None, "complex_name": None, 
#             "property_type": None, "price_metric": None, 
#             "start_date": None, "end_date": None
#         }

# field_extractor = FieldExtractor()

# gemini
import json
import re
from datetime import datetime, timedelta
from core.gateway.adapters import gemini_engine

class FieldExtractor:
    def __init__(self):
        pass

    def _clean_json_response(self, response: str) -> str:
        """AI의 응답에서 순수 JSON 포맷만 추출한다."""
        try:
            # UI 파싱 오류를 막기 위해 문자열 곱셈(` * 3)과 정규식 `{3} 활용
            if "`" * 3 in response:
                pattern = r"`{3}(?:json)?\s*(\{.*?\})\s*`{3}"
                match = re.search(pattern, response, re.DOTALL)
                if match: return match.group(1)
            start = response.find("{")
            end = response.rfind("}")
            if start != -1 and end != -1: return response[start : end + 1]
            return response
        except:
            return response

    def extract(self, user_query: str, session_id: str = None) -> dict:
        """자연어 질의에서 DB 검색용 필드를 JSON 형태로 추출한다."""
        # 1. 오늘 날짜를 YYYY-MM-DD 형식으로 동적 할당한다.
        today_str = datetime.today().strftime('%Y-%m-%d')
        
        # 2. 시스템 프롬프트를 정의한다. (기간을 start_date, end_date로 분리 지시)
        prompt = f"""
[역할]
사용자의 질문에서 검색 키워드를 추출하여 JSON으로 반환한다.
값이 없으면 null로 표기한다.

[특별 지시사항: 지역명 유추]
사용자가 '신나무실신성', '은마아파트'처럼 특정 아파트 단지명만 언급하고 지역명을 생략한 경우, 당신의 배경지식을 활용하여 해당 아파트가 위치한 정확한 시/구(예: 수원시 영통구, 서울시 강남구)를 `location` 필드에 반드시 채워 넣는다.

[기준 정보]
오늘 날짜는 {today_str}이다. "최근 한 달", "올해" 등의 표현은 이 날짜를 기준으로 계산한다.

[예시]
질문: "용인시 수지구 아파트 최근 한달 전세 시세"
JSON: {{"location": "용인시 수지구", "complex_name": null, "property_type": "아파트", "price_metric": "전세", "start_date": "2026-02-28", "end_date": "2026-03-31", "main_intent": "시세"}}

질문: "수지구 26년 1월부터 3월 10일까지 시세"
JSON: {{"location": "수지구", "complex_name": null, "property_type": "아파트", "price_metric": "매매", "start_date": "2026-01-01", "end_date": "2026-03-10", "main_intent": "시세"}}

[필드 가이드]
1. location: 지역명 (예: 용인, 수지구).
2. complex_name: 아파트 단지명.
3. property_type: 아파트, 오피스텔.
4. price_metric: 매매, 전세, 월세.
5. start_date: 검색 시작일 (YYYY-MM-DD 형식). 특정할 수 없으면 null.
6. end_date: 검색 종료일 (YYYY-MM-DD 형식). 특정할 수 없으면 null.
7. main_intent: 시세, 전망.

[사용자 질문]
"{user_query}"

[출력]
오직 JSON만 반환한다.
"""
        extracted_data = {}
        try:
            # 3. AI 모델에 프롬프트를 전송하고 응답을 수신한다.
            raw_response = gemini_engine.generate(prompt)
            print(f"🔥 [DEBUG] AI 원본 응답: {raw_response}")
            
            # 4. JSON 파싱을 수행한다.
            cleaned = self._clean_json_response(raw_response)
            extracted_data = json.loads(cleaned)
        except Exception as e:
            print(f"⚠️ [AI 실패] 파이썬 강제 보정 모드로 전환함. ({e})")
            extracted_data = {}

        # =========================================================
        # 🚑 [파이썬 강제 보정 로직]
        # =========================================================
        
        # 1. 필수 키 구조를 초기화한다. (period 삭제, start_date 및 end_date 추가)
        for key in ["location", "complex_name", "property_type", "price_metric", "start_date", "end_date", "main_intent"]:
            if key not in extracted_data: extracted_data[key] = None

        # 2. 가격 기준을 강제 주입한다.
        if not extracted_data["price_metric"]:
            if "전세" in user_query: extracted_data["price_metric"] = "전세"
            elif "월세" in user_query: extracted_data["price_metric"] = "월세"
            elif "매매" in user_query: extracted_data["price_metric"] = "매매"
            else: extracted_data["price_metric"] = "매매" 

        # 3. 부동산 유형을 강제 주입한다.
        if not extracted_data["property_type"]:
            if "오피" in user_query: extracted_data["property_type"] = "오피스텔"
            elif "빌라" in user_query: extracted_data["property_type"] = "빌라"
            else: extracted_data["property_type"] = "아파트" 

        # 4. 지역을 강제 주입한다. (정규표현식 활용)
        if not extracted_data["location"]:
            loc_pattern = r"([가-힣]+(?:시|구|동))"
            match = re.search(loc_pattern, user_query)
            if match: extracted_data["location"] = match.group(1)

        # 5. 날짜(기간) 강제 주입 (디폴트 설정)
        # 사용자가 날짜를 말하지 않아 Null일 경우, '최근 6개월'을 기본 검색 기간으로 설정한다.
        today = datetime.today()
        
        if not extracted_data.get("end_date"):
            # 종료일이 없으면 오늘 날짜로 세팅
            extracted_data["end_date"] = today.strftime('%Y-%m-%d')
            
        if not extracted_data.get("start_date"):
            # 시작일이 없으면 오늘 기준 90일(약 3개월) 전으로 세팅
            past_date = today - timedelta(days=90)
            extracted_data["start_date"] = past_date.strftime('%Y-%m-%d')

        # 6. 의도를 강제 주입한다. (기본값 설정)
        if not extracted_data["main_intent"]:
            # 질문에 '전망'이나 '예측'이 들어가면 main_intent를 '전망'으로 바꾼다.
            if "전망" in user_query or "예측" in user_query or "어때" in user_query:
                extracted_data["main_intent"] = "전망"
            else:
                extracted_data["main_intent"] = "시세"

        print(f"✅ [최종 추출 결과] {extracted_data}")
        return extracted_data

field_extractor = FieldExtractor()