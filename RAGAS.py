import os
import pandas as pd
from datasets import Dataset

# RAGAS 핵심 평가 지표 임포트
from ragas import evaluate
from ragas.metrics import faithfulness, answer_relevancy

# ------------------------------------------------------------------------------
# 1. [필수 설정] 평가관(Judge) LLM을 위한 OpenAI API 키
# ------------------------------------------------------------------------------
# 임베딩은 KO-SBERT(무료)를 쓰셨더라도, RAGAS가 채점하려면 똑똑한 LLM이 필요합니다.
# 여기에 채점용 OpenAI API 키를 입력해 주세요.
os.environ["OPENAI_API_KEY"] = "sk-여기에_채점용_API키를_입력하세요"

# ------------------------------------------------------------------------------
# 2. 최신 RAGAS v0.2+ 규격에 맞춘 데이터셋 준비
# ------------------------------------------------------------------------------

data_samples = {
    "user_input": [
        "2026년 1월에 계약된 광교 래미안 30평 전세가는 얼마야?", 
        "용인시 수지구에 있는 아파트 월세 매물 있어?",
        "수원시 권선구 아파트 매매가 알려줘."
    ],
    "retrieved_contexts": [
        ["수원시 영통구 광교 래미안 30평 전세가: 4억 원. 계약일: 2026-01-11", "광교 래미안 단지 정보..."],
        ["용인시 수지구 성복동 자이 40평 보증금 1억, 월세 150만 원. 계약일: 2026-01-10"],
        ["수원시 권선구 아이파크 33평 전세 3억 원."]
    ],
    "response": [
        "2026년 1월 기준, 광교 래미안 30평의 전세가는 4억 원입니다.", 
        "네, 용인시 수지구 성복동 자이 40평 매물이 있으며 보증금 1억에 월세 150만 원입니다.",
        "수원시 권선구 아이파크 33평의 매매가는 3억 원입니다."
    ],
    "reference": [
        "광교 래미안 30평 전세가는 4억 원입니다.",
        "용인시 수지구 성복동 자이 40평 보증금 1억, 월세 150만 원입니다.",
        "DB에 제공된 데이터 중 매매가 정보는 없습니다."
    ]
}

dataset = Dataset.from_dict(data_samples)

# ------------------------------------------------------------------------------
# 3. RAGAS 평가 실행
# ------------------------------------------------------------------------------
print("🚀 [System] RAGAS 평가를 시작합니다. (LLM이 채점 중...)")

result = evaluate(
    dataset=dataset,
    metrics=[
        faithfulness,      
        answer_relevancy,  
    ],
)

# ------------------------------------------------------------------------------
# 4. [수정됨] 결과 출력 및 엑셀(CSV) 저장
# ------------------------------------------------------------------------------
print("\n✅ [System] 평가 완료! 전체 평균 점수:")
print(result) 

# (기존 코드)
df_result = result.to_pandas()

# ------------------------------------------------------------------------------
# 🎯 [추가] 발표용 '환각률(%)' 수치화 파생 변수 만들기
# ------------------------------------------------------------------------------
# faithfulness가 NaN(계산 불가)인 경우 0으로 처리한 뒤 환각률 계산
df_result['faithfulness'] = df_result['faithfulness'].fillna(0)
df_result['hallucination_rate(%)'] = (1.0 - df_result['faithfulness']) * 100

print("\n📊 [세부 채점 결과 (환각률 포함)]")
print(df_result[['user_input', 'faithfulness', 'hallucination_rate(%)']])

# 전체 평균 환각률 계산 (발표 대본용)
total_hallucination = df_result['hallucination_rate(%)'].mean()
print(f"\n🏆 [최종 결과] 우리 RAG 시스템의 평균 환각률은 {total_hallucination:.2f}% 입니다!")

# 엑셀 저장
df_result.to_csv("ragas_evaluation_results.csv", index=False, encoding='utf-8-sig')

print("\n📊 [세부 채점 결과]")
# 최신 규격에 맞춰 'user_input'으로 출력하도록 수정했습니다.
print(df_result[['user_input', 'faithfulness', 'answer_relevancy']])

df_result.to_csv("ragas_evaluation_results.csv", index=False, encoding='utf-8-sig')
print("\n💾 [System] 평가 결과가 'ragas_evaluation_results.csv'로 저장되었습니다.")