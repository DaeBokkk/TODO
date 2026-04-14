import requests
import json
import os
import sys

# [1] Gemini 엔진 가져오기
try:
    from core.gateway.adapters import gemini_engine
except ImportError:
    print("❌ [오류] 'core/gateway/adapters.py'를 찾을 수 없습니다.")
    sys.exit(1)

# [2] 검색 서버 설정 (최신 주소 반영)
SEARCH_SERVER_URL = "http://3.39.23.25:8000/hybrid_search"

class SearchClient:
    """
    AWS 검색 서버와 통신하여 실거래 데이터를 수집하는 역할을 수행함.
    """
    def __init__(self):
        self.search_url = SEARCH_SERVER_URL

    def fetch_real_estate_data(self, user_query: str) -> str:
        """
        사용자 질문을 검색 서버에 전달하고 결과를 텍스트 형식으로 반환함.
        """
        context_text = ""
        try:
            payload = {"query": user_query}
            response = requests.post(self.search_url, json=payload, timeout=20)
            
            if response.status_code == 200:
                results = response.json()
                # 데이터 형식에 따른 파싱 처리
                if isinstance(results, list):
                    context_text = "\n".join([str(item) for item in results])
                elif isinstance(results, dict):
                    context_text = results.get("results", str(results))
                else:
                    context_text = str(results)
            else:
                context_text = "관련 자료 없음"
        except Exception as e:
            print(f"\n❌ [연결 오류] 검색 서버 접속 실패: {e}")
            context_text = "서버 연결 실패로 인해 자료 확보 불가"
            
        return context_text

# 다른 파일에서 불러다 쓸 수 있도록 객체 생성
search_client = SearchClient()

def run_hybrid_rag_system():
    """
    CLI 환경에서 검색과 생성을 통합 테스트하는 루프를 실행함.
    """
    print("\n" + "="*70)
    print("🚀 [최종 통합] 원격 검색(AWS) + 로컬 생성(Gemini) 시스템")
    print(f"📡 검색 서버: {SEARCH_SERVER_URL}")
    print("="*70)

    while True:
        user_query = input("\n👤 질문: ").strip()
        if not user_query: continue
        if user_query.lower() in ['q', 'quit', 'exit']:
            print("👋 시스템을 종료합니다.")
            break

        print(f"   🔍 AWS 서버에서 자료를 검색하고 있습니다...", end="", flush=True)

        # 서치 클라이언트를 사용하여 데이터 확보
        context_text = search_client.fetch_real_estate_data(user_query)
        print("\r", end="")

        if "자료 확보 불가" not in context_text:
            print(f"📚 [검색 완료] 참고 자료 확보 ({len(context_text)}자)")
        
        print(f"   🤖 Gemini가 답변을 생성 중입니다...", end="", flush=True)
        
        try:
            # 다정한 페르소나를 반영한 프롬프트 구성
            final_prompt = f"""
옆에 있는 고객에게 친절하게 설명해주는 '부동산 전문가'가 되어 답변해주세요.

[검색 자료]
{context_text}

[질문]
{user_query}

[지침]
- "~해요", "~이네요" 같은 부드러운 말투를 사용하세요.
- 중요한 가격 정보는 **볼드체**로 강조하세요.
- 자료가 없으면 정중하게 양해를 구하세요.
"""
            final_answer = gemini_engine.generate(final_prompt)
            print("\r", end="") 
            
            print(f"\n🤖 AI 답변:\n{final_answer}")
            print("-" * 60)

        except Exception as e:
            print(f"\n❌ [생성 오류] Gemini 에러: {e}")

if __name__ == "__main__":
    run_hybrid_rag_system()