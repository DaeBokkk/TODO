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