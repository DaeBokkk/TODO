class QueryGuidance:
    def __init__(self):
        pass

    def detect_missing(self, extracted_data: dict) -> list:
        """
        [설계서 3.4 & 팀장님 요청 반영]
        모든 필드를 개별적으로 엄격하게 검사합니다.
        """
        missing = []
        
        # 1. [수정됨] 위치 정보 (단지명이 있더라도 지역은 무조건 필수!)
        if not extracted_data.get("location"):
            missing.append("location")
        
        # 2. 부동산 유형 (단지명이 있으면 아파트로 유추 가능하므로, 이건 예외 인정)
        # (만약 유형도 무조건 받고 싶으시면 'and not ...' 부분을 지우면 됩니다)
        has_complex = extracted_data.get("complex_name")
        if not extracted_data.get("property_type") and not has_complex:
            missing.append("property_type")

        # 3. 가격 기준 (매매/전세/월세) - 필수
        if not extracted_data.get("price_metric"):
            missing.append("price_metric")

        # 4. 조회 기간 - 필수
        if not extracted_data.get("period"):
            missing.append("period")
            
        return missing

    def get_guidance_message(self, missing_fields: list) -> str:
        """
        빠진 정보들을 조합하여 자연스러운 되묻기 질문을 생성합니다.
        """
        questions = []

        # 1. 지역 (가장 중요)
        if "location" in missing_fields:
            questions.append("지역(구/동)")
        
        # 2. 유형
        if "property_type" in missing_fields:
            questions.append("부동산 유형")

        # 3. 가격 기준
        if "price_metric" in missing_fields:
            questions.append("가격 기준(매매/전세)")

        # 4. 기간
        if "period" in missing_fields:
            questions.append("조회 기간")

        if not questions:
            return None

        # 자연어 조합
        required_str = ", ".join(questions)
        return f"정확한 검색을 위해 '{required_str}' 정보를 더 알려주시겠어요?"

guidance_module = QueryGuidance()