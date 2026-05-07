import os
import ast
import time
import pandas as pd
from datasets import Dataset
import dotenv

# LangChain 및 RAGAS 임포트
from langchain_openai import ChatOpenAI
from ragas import evaluate
from ragas.metrics import (
    faithfulness,
    answer_relevancy,
    context_recall,
    context_precision,
    answer_correctness
)

# ------------------------------------------------------------------------------
# 1. 환경 설정 및 가성비 평가관(LLM) 셋팅
# ------------------------------------------------------------------------------
dotenv.load_dotenv()
os.environ["OPENAI_API_KEY"] = os.getenv('Emb_KEY') 

# 💡 [핵심] 비용을 1/10로 줄이고 속도를 높인 gpt-4o-mini 모델로 평가관 지정
print(" [System] 채점관 LLM을 'gpt-4o-mini'로 설정합니다. (비용 절감 및 속도 최적화)")
cheap_judge_llm = ChatOpenAI(model_name="gpt-4o-mini")

# ------------------------------------------------------------------------------
# 2. CSV 파일 로드 및 전처리
# ------------------------------------------------------------------------------
# [수정 1] 테스트 계획서에 맞게 파일명 변경 (80개 시나리오)
CSV_FILE_PATH = "/Users/solseon/Desktop/TODO/ragas_80_detail_recordno3.csv"

print(f"📂 [System] '{CSV_FILE_PATH}' 파일에서 데이터를 불러옵니다...")

try:
    df = pd.read_csv(CSV_FILE_PATH)
except FileNotFoundError:
    print(f"❌ [Error] {CSV_FILE_PATH} 파일을 찾을 수 없습니다. 경로를 확인해주세요.")
    exit()

# 필수 컬럼 존재 여부만 확인 (question_type 등을 지우지 않고 살려둡니다)
required_columns = ['user_input', 'retrieved_contexts', 'response', 'reference']
for col in required_columns:
    if col not in df.columns:
        raise ValueError(f"❌ [Error] CSV 파일에 필수 컬럼 '{col}'이 없습니다.")

# [수정 2] 기존의 df = df[required_columns] 부분을 삭제했습니다. 
# 이제 question_type이나 latency 같은 여분 컬럼이 최종 결과 엑셀에도 그대로 유지됩니다!

# 문자열로 된 검색결과 리스트를 실제 Python 리스트로 변환
def parse_contexts(context_str):
    if isinstance(context_str, str):
        try:
            return ast.literal_eval(context_str)
        except (ValueError, SyntaxError):
            return [context_str]
    return context_str

df['retrieved_contexts'] = df['retrieved_contexts'].apply(parse_contexts)
dataset = Dataset.from_pandas(df)
total_data = len(dataset)
print(f"✅ [System] 총 {total_data}개의 테스트 케이스 로드 완료.\n")

# ------------------------------------------------------------------------------
# 3. 20개 단위 분할 평가 (Rate Limit 방어)
# ------------------------------------------------------------------------------
batch_size = 20
all_eval_results = []

print(f"🚀 [System] Rate Limit 방어를 위해 {batch_size}개씩 나누어 평가를 시작합니다.")
print("="*50)

for i in range(0, total_data, batch_size):
    end_idx = min(i + batch_size, total_data)
    
    # 데이터셋 자르기 (Slice)
    batch_dataset = dataset.select(range(i, end_idx))
    print(f"▶️ [{i+1} ~ {end_idx}] 번째 문항 채점 중...")
    
    # 20개만 RAGAS 평가 진행 (이때 llm=cheap_judge_llm 적용)
    batch_result = evaluate(
        dataset=batch_dataset,
        metrics=[faithfulness, answer_relevancy, context_recall, context_precision, answer_correctness],
        llm=cheap_judge_llm
    )
    
    # 평가 결과를 Pandas DataFrame으로 변환하여 리스트에 보관
    all_eval_results.append(batch_result.to_pandas())
    
    # 마지막 배치가 아니면 15초 대기 (API 숨고르기)
    if end_idx < total_data:
        print(f"   ⏳ 과부하 방지를 위해 15초간 대기합니다... (진행률: {end_idx}/{total_data})")
        time.sleep(15)

print("="*50)
print("✅ [System] 모든 문항의 채점이 완료되었습니다!\n")

# ------------------------------------------------------------------------------
# 4. 흩어진 결과 하나로 합치기 및 파생 변수 계산
# ------------------------------------------------------------------------------
# 리스트에 모인 20개짜리 결과 조각들을 하나로 합침
final_df = pd.concat(all_eval_results, ignore_index=True)

# 환각률(%) 계산 (결측치는 0으로 처리)
final_df['faithfulness'] = final_df['faithfulness'].fillna(0)
final_df['hallucination_rate(%)'] = (1.0 - final_df['faithfulness']) * 100

# ------------------------------------------------------------------------------
# 5. 최종 리포트 출력 및 CSV 저장
# ------------------------------------------------------------------------------
print("🏆 [최종 시스템 평가 평균 점수]")
print(f"   - 사실 충실도 (Faithfulness): {final_df['faithfulness'].mean():.4f}")
print(f"   - 답변 관련성 (Answer Relevancy): {final_df['answer_relevancy'].mean():.4f}")
print(f"   - 평균 환각률 (Hallucination Rate): {final_df['hallucination_rate(%)'].mean():.2f}%")
print(f"   - 검색 재현율 (Context Recall): {final_df['context_recall'].mean():.4f}")
print(f"   - 검색 정밀도 (Context Precision): {final_df['context_precision'].mean():.4f}")
print(f"   - 정답 정확도 (Answer Correctness): {final_df['answer_correctness'].mean():.4f}")
print("="*50)

# [수정 3] 세부 내역 저장 파일명을 80개에 맞게 변경
detail_file = "ragas_80_detail_report.csv"
final_df.to_csv(detail_file, index=False, encoding='utf-8-sig')

# 평균 요약본 저장 (발표용)
summary_data = {
    "Metrics": ["Faithfulness", "Answer Relevancy", "Context Recall", 
                "Context Precision", "Answer Correctness", "Hallucination Rate(%)"],
    "Average_Score": [
        final_df['faithfulness'].mean(), final_df['answer_relevancy'].mean(),
        final_df['context_recall'].mean(), final_df['context_precision'].mean(),
        final_df['answer_correctness'].mean(), final_df['hallucination_rate(%)'].mean()
    ]
}
summary_df = pd.DataFrame(summary_data)
summary_file = "ragas_average_summary.csv"
summary_df.to_csv(summary_file, index=False, encoding='utf-8-sig')

print(f"💾 [System] 문항별 세부 결과가 '{detail_file}'로 저장되었습니다.")
print(f"💾 [System] 전체 평균 요약본이 '{summary_file}'로 저장되었습니다.")