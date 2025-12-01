# test_client.py

import requests
import json

# 1. 요청 보낼 주소 (로컬 서버)
url = "https://subinternal-decompressive-gunner.ngrok-free.dev/hybrid_search"

# 2. 보낼 데이터 (검색 조건)
# region_name은 DB에 실제로 있는 지역명이어야 합니다.
payload = {
    "query": "경기도 평택시 평균 아파트 시세 알려줘",
    "region_name": "평택",       # [중요] DB에 있는 지역명인지 확인
    "date_start": "2025-01-01",  # 범위를 넓게 잡아서 데이터가 잡히는지 확인
    "date_end": "2025-12-31",    # 오늘 날짜 이후까지 넉넉하게
    "k": 3
}

try:
    # 3. POST 요청 전송
    print(f"🚀 요청 전송 중... (Target: {payload['region_name']})")
    response = requests.post(url, json=payload)
    
    # 4. 응답 확인
    if response.status_code == 200:
        results = response.json()
        print(f"✅ 서버 응답 성공! (결과 개수: {len(results)}개)")
        
        # 결과가 비어있다면 출력
        if not results:
            print("⚠️ 결과가 [] (빈 리스트)입니다. 서버 터미널의 로그를 확인하세요.")
        else:
            print(json.dumps(results, indent=2, ensure_ascii=False))
            
    else:
        print(f"❌ 에러 발생: {response.status_code}")
        print(response.text)

except Exception as e:
    print(f"❌ 연결 실패: {e}")
    print("서버가 켜져 있는지 확인하세요 (uvicorn server:app ...)")