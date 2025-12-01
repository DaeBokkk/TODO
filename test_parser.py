# test_parser.py

import requests
import json

url = "http://127.0.0.1:8000/hybrid_search_debug"

payload = {
    "main_intent": "실거래가 알려줘",
    "location": "평택시",          
    "complex_name": "", 
    "property_type": "아파트",
    "price_metric": "전세",
    "period": "2025-11-01~2025-11-29", # 기간 넉넉하게
    "k": 3
}

print(f"🚀 [Test -> Server(Debug)] 상세 데이터 요청 중...")

try:
    response = requests.post(url, json=payload)
    
    if response.status_code == 200:
        results = response.json()
        print(f"✅ 수신 성공 ({len(results)}건)\n")
        
        for idx, item in enumerate(results, 1):
            print(f"[{idx}] Score: {item['score']:.4f}")
            print(f"    📅 날짜: {item['metadata']['contract_date']}")
            print(f"    📍 지역: {item['metadata']['region_name']}")
            print(f"    🆔 ID : {item['metadata']['source_id']}")
            print(f"    📄 내용: {item['content']}") # 내용 너무 기니까 앞부분만
            print("-" * 50)
            
    else:
        print(f"❌ 에러: {response.status_code}")
        print(response.text)
        
except Exception as e:
    print(f"❌ 연결 실패: {e}")