import requests
import json

# [중요] 멤버 B 서버 주소
MEMBER_B_API_URL = "https://subinternal-decompressive-gunner.ngrok-free.dev/hybrid_search"

class RemoteRetriever:
    def search(self, query_frame: dict, user_query: str) -> str:
        """
        팀장님의 6개 필드를 매핑 없이 그대로 전송합니다.
        """
        print(f"\n📡 [연결] 멤버 B({MEMBER_B_API_URL})에게 데이터 전송 중...")
        
        # 1. query 구성
        # 딕셔너리 언패킹(**)을 사용하여 팀장님의 필드(location, property_type 등)를 그대로 넣습니다.
        query = {
            "query": user_query,          # 원본 질문
            **query_frame,                # [핵심] 파싱된 6개 필드 그대로 전송 (location 등)
        }
        
        print(f"📦 [전송 데이터] {json.dumps(query, ensure_ascii=False)}")
        
        try:
            # 2. 전송
            response = requests.post(MEMBER_B_API_URL, json=query, timeout=20)
            
            # 3. 응답 처리
            if response.status_code == 200:
                results = response.json()
                
                # [디버깅 로그] 멤버 B가 뭘 보냈는지 눈으로 확인!
                print(f"📥 [수신 데이터 확인] 타입: {type(results)}")
                # print(f"📥 [수신 데이터 내용] {results}") # 내용이 너무 길면 주석 처리
                
                context = ""
                
                # Case A: 리스트인 경우
                if isinstance(results, list):
                    if not results: # 빈 리스트면
                        context = "검색 결과가 없습니다."
                    
                    # A-1: 문자열 리스트 ["내용1", "내용2"] 인 경우 (여기서 에러 났었음!)
                    elif isinstance(results[0], str):
                        context = "\n\n".join(results)
                    
                    # A-2: 딕셔너리 리스트 [{"content":...}, {"content":...}] 인 경우 (원래 약속)
                    elif isinstance(results[0], dict):
                        context = "\n\n".join([item.get("content", str(item)) for item in results])
                    
                    # A-3: 기타 (그냥 문자열로 변환)
                    else:
                        context = str(results)

                # Case B: 딕셔너리인 경우 ({"context": "..."})
                elif isinstance(results, dict):
                    context = results.get("context") or results.get("answer") or str(results)
                
                # Case C: 기타
                else:
                    context = str(results)
                    
                print(f"✅ [성공] 문서 {len(context)}자 확보!")
                return context
            
            else:
                print(f"❌ [실패] 서버 에러: {response.status_code} - {response.text}")
                return "검색 서버 오류가 발생했습니다."
                
        except Exception as e:
            print(f"❌ [연결 실패] {e}")
            return "검색 서버에 연결할 수 없습니다."

# 전역 인스턴스 생성
retrieval_service = RemoteRetriever()