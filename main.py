from fastapi import FastAPI, Request
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import requests
import sys
from datetime import datetime

# [1] 모듈 가져오기
try:
    from modules.preprocessing.extractor import field_extractor
    from core.gateway.adapters import gemini_engine
    from core.memory.history import history_manager  # 멀티턴 기억장치 추가
except ImportError as e:
    print(f"❌ [오류] 모듈 로딩 실패. 프로젝트 루트에서 실행한다. {e}")
    sys.exit(1)

# [2] 앱 생성 및 설정
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

SEARCH_SERVER_URL = "http://3.39.23.25:8000/hybrid_search"

@app.post("/v1/chat")
async def chat_endpoint(request: Request):
    try:
        body = await request.json()
        print(f"\n👀 [도착한 데이터 확인] {body}") 
        
        user_query = ""
        session_id = "default_session"
        
        # UI 요청 데이터 형식 파싱 및 세션 ID 추출
        if "prompt" in body and isinstance(body["prompt"], dict):
            user_query = body["prompt"].get("user", "")
            session_id = body["prompt"].get("session_id", "default_session")

        if not user_query:
            user_query = body.get("query") or body.get("message") or body.get("content", "")

        if not user_query and "messages" in body:
             user_query = body["messages"][-1]["content"]
             
        print(f"📩 [최종 해석된 질문] {user_query}")
        
        if not user_query:
            return {"answer": "오류: 질문 내용을 찾을 수 없다."}
        
        # 1. 사용자의 질문을 기억장치에 저장한다.
        history_manager.add_user_message(session_id, user_query)

        # 2. 전처리 모듈을 통해 엔티티를 추출한다.
        extracted = field_extractor.extract(user_query)
        
        # 3. 추출된 데이터를 이전 상태와 병합한다. (멀티턴 슬롯 필링 적용)
        current_state = history_manager.update_state(session_id, extracted)
        
        payload = current_state.copy()
        payload["query"] = user_query
        for k, v in payload.items():
            if v is None: payload[k] = ""
        print(f"  ⚙️ [1단계] 누적 및 추출 완료: {payload}")

        # 4. AWS 검색 서버에 하이브리드 검색을 요청한다.
        context_text = ""
        print(f"  📡 [2단계] AWS 검색 서버 호출 중...")
        res = requests.post(SEARCH_SERVER_URL, json=payload, timeout=180)
        
        if res.status_code == 200:
            results = res.json()
            print(f"  🕵️ [DEBUG] AWS 실제 응답 원본: {results}") 
            
            contexts = results.get("contexts", [])
            
            if contexts: 
                # 유효한 데이터가 존재할 경우 문자열로 결합한다.
                context_text = "\n".join([str(c) for c in contexts])
                print(f"  📚 데이터 확보 성공 ({len(context_text)}자)")
            else: 
                # 검색 결과가 0건일 경우 예외 텍스트를 주입한다.
                context_text = "현재 데이터베이스에 해당 기간의 정보가 업데이트되지 않았다."
                print(f"  ⚠️ 검색 결과 0건 (AWS 서버에서 매칭되는 데이터를 찾지 못함)")
                
        else:
            context_text = "관련 자료 없음"
            print(f"  ⚠️ 검색 실패 (상태 코드: {res.status_code})")

        # 5. 생성 모델(Gemini)을 통해 최종 답변을 생성한다.
        print(f"  🤖 [3단계] 답변 생성 중...")
        today_str = datetime.today().strftime('%Y년 %m월 %d일')
        
        # 과거 대화 기록을 프롬프트용 문자열로 불러온다.
        chat_history = history_manager.get_context_string(session_id)

        prompt = f"""
[역할]
당신은 부동산 전문가이다. 사용자가 보기 편하게 가독성을 최우선으로 답변한다.
오늘 날짜는 {today_str}이다. 사용자가 언급하는 연도(예: 2026년)는 미래가 아닌 현재 또는 과거이므로, 절대 미래의 날짜라고 언급하거나 답변을 거부하지 않는다.

[이전 대화 기록]
{chat_history}

[데이터 활용 지침 (매우 중요)]
1. 사용자가 대명사("여기", "저 아파트들" 등)를 사용하거나 맥락을 이어가는 질문을 하면, [이전 대화 기록]과 [참고 자료]를 종합하여 답변한다.
2. 제공된 [참고 자료]가 사용자가 질문한 지역이나 아파트와 일치한다면, 해당 데이터를 적극 활용하여 분석한다.
3. 만약 [참고 자료]에 있는 데이터가 사용자가 질문한 지역/아파트와 무관하거나 엉뚱한 지역의 데이터라면, 이 참고 자료는 완전히 무시한다.
4. 참고 자료를 무시한 경우, "현재 데이터베이스에 해당 단지의 최근 실거래 내역이 없어 일반적인 전문가 분석을 제공합니다"라고 안내한 뒤, 당신이 가진 사전 지식(학군, 호재, 입지 분석 등)을 총동원하여 최고 품질의 부동산 전망 리포트를 작성한다.

[참고 자료]
{context_text}

[현재 질문]
{user_query}

[출력 지침]
1. 반드시 불렛 포인트(-)와 빈 줄(개행)을 적극적으로 사용하여 가독성을 극대화한다.
2. 여러 개의 아파트 매물 정보를 나열할 때는 반드시 각 매물 정보 사이에 마크다운 구분선(---)과 빈 줄을 추가하여 시각적으로 완벽하게 분리되도록 한다.
3. 아파트 매물 정보는 반드시 아래의 양식을 엄격하게 준수하여 작성한다.

   (출력 양식 예시):
   🔹 **[아파트명]** (동 이름)
   - **거래일자**: 2026년 03월 20일
   - **거래금액**: 4억 5,000만원
   - **전용면적**: 84.87㎡ (약 25.7평)
   - **층수**: 6층
   - **준공연도**: 2002년
   
   ---
   
   🔹 **[아파트명]** (동 이름)
   - **거래일자**: ...
   (반복)

4. 시장 분석 및 전망 부분도 문단이 바뀔 때마다 반드시 한 줄을 띄어 써서(빈 줄 삽입) 답답해 보이지 않게 작성한다.
5. 서술형 줄글로 뭉뚱그려 쓰지 말고, 답변은 한국어로 정중하게 작성한다.
"""
        answer = gemini_engine.generate(prompt)
        
        # 생성된 AI의 응답을 다음 세션을 위해 기억장치에 저장한다.
        history_manager.add_system_message(session_id, answer)
        
        print(f"  ✅ 답변 완료!")
        return {"answer": answer, "choices": [{"message": {"content": answer}}]} 

    except Exception as e:
        print(f"❌ 에러 발생: {e}")
        return {"answer": f"서버 에러: {str(e)}"}
    
if __name__ == "__main__":
    print("🚀 [서버 가동] 프론트엔드 UI 접속 대기 중...")
    uvicorn.run(app, host="0.0.0.0", port=8000)