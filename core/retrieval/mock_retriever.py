class MockRetriever:
    def __init__(self):
        # 임시 데이터베이스
        self.database = {
            "강남구": "서울 강남구 아파트 평균 매매가는 25억 5천만 원이며, 전세가율은 45%입니다. 주요 단지로는 은마, 압구정 현대가 있습니다.",
            "서초구": "서울 서초구 반포자이 84제곱미터 최근 실거래가는 35억 원입니다. 전세 매물이 부족하여 가격이 강세입니다.",
            "마포구": "서울 마포구 아파트 평균 매매가는 14억 원입니다. 마포래미안푸르지오가 시세를 주도하고 있습니다."
        }

    def search(self, location: str, property_type: str) -> str:
        print(f"🔎 [Retriever] 검색 시도: {location}, {property_type}")
        
        # 지역명으로 데이터 찾기 (없으면 기본값)
        if location and location in self.database:
            return self.database[location]
        
        return "해당 지역에 대한 구체적인 데이터가 없습니다. 서울시 평균 시세를 참고하세요."

# 인스턴스 생성
mock_retriever = MockRetriever()
